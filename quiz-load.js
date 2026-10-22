// haequiz 실서버 부하 테스트 — 참여자 N명(기본 200)이 PIN 으로 들어와 문제를 푼다.
// 방 만들기 · 진행(시작 · 마감 · 다음 · 결과 발표 · 방 종료)은 운영자가 진행 화면에서 직접 한다.
// 참여자 한 명 = 웹소켓 하나 (태블릿과 같은 SockJS websocket 경로 · STOMP · 10초 하트비트).
//
// 실행: k6 run -e PIN=123456 quiz-load.js      (자세한 방법은 README.md)
import { WebSocket } from 'k6/websockets';
import { setTimeout, clearTimeout, setInterval, clearInterval } from 'k6/timers';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.1.0/index.js';

const PIN = (__ENV.PIN || '').trim();
const API = __ENV.API || 'https://api.goldenbellquiz.site';
const ORIGIN = __ENV.ORIGIN || 'https://goldenbellquiz.site';
const WS_BASE = API.replace(/^http/, 'ws') + '/ws';
const PLAYERS = Number(__ENV.PLAYERS || 200);
const JOIN_MS = Number(__ENV.JOIN_SEC || 30) * 1000;           // 이 시간에 걸쳐 나눠서 입장
const THINK_MIN = Number(__ENV.THINK_MIN_SEC || 1) * 1000;     // 문제를 받고 답하기까지
const THINK_MAX = Number(__ENV.THINK_MAX_SEC || 6) * 1000;
const HOLD_MS = Number(__ENV.HOLD_MIN || 60) * 60_000;         // 최대 접속 유지 시간 (방이 닫히면 먼저 끝난다)
const NAME = __ENV.NAME_PREFIX || '부하';                       // 같은 방에서 다시 돌릴 때는 바꾼다
const MAX_Q = 50;

if (!/^\d{6}$/.test(PIN)) throw new Error('PIN 6자리를 넣어 주세요: k6 run -e PIN=123456 quiz-load.js');

const tConnect = new Trend('hq_connect_ms', true);   // 웹소켓 열림 → STOMP 연결 완료
const tJoin = new Trend('hq_join_ms', true);         // 입장 요청 → 응답
const tAnswer = new Trend('hq_answer_ms', true);     // 답 제출 → 응답
const cJoined = new Counter('hq_joined');
const cAnswered = new Counter('hq_answered');
const cQuestions = new Counter('hq_questions_received');
const cResults = new Counter('hq_results_received');
const cEnded = new Counter('hq_ended_received');
const cFail = new Counter('hq_fail');                // 응답 실패 · STOMP ERROR · 연결 오류
const cLate = new Counter('hq_late_answers');        // 진행자가 먼저 마감해서 늦게 낸 답 (실패 아님)
const cVideoOrder = new Counter('hq_video_order');   // 영상 신호보다 문제가 먼저 와서 잠긴 문제에 답함 (서버 순서 문제)
// 문제 · 결과가 도착한 시각(이 PC 시계)과 받은 인원. 끝나고 '첫 도착 ~ 마지막 도착' 차이로 다 받기까지 걸린 시간을 본다
const qArrival = Array.from({ length: MAX_Q }, (_, i) => new Trend(`hq_arrival_q${i + 1}`));
const rArrival = Array.from({ length: MAX_Q }, (_, i) => new Trend(`hq_arrival_r${i + 1}`));
const qCount = Array.from({ length: MAX_Q }, (_, i) => new Counter(`hq_count_q${i + 1}`));
const rCount = Array.from({ length: MAX_Q }, (_, i) => new Counter(`hq_count_r${i + 1}`));

export const options = {
  scenarios: {
    players: { executor: 'per-vu-iterations', vus: PLAYERS, iterations: 1, maxDuration: `${HOLD_MS + JOIN_MS + 60_000}ms` },
  },
  thresholds: {
    hq_join_ms: ['p(95)<1000'],
    hq_answer_ms: ['p(95)<500'],
    hq_fail: ['count==0'],
  },
};

/* ---------------- STOMP over SockJS ---------------- */

function stompFrame(command, headers, body = '') {
  let s = command + '\n';
  for (const k in headers) s += `${k}:${headers[k]}\n`;
  return s + '\n' + body + '\0';
}

function parseStomp(raw) {
  const out = [];
  for (const chunk of raw.split('\0')) {
    const s = chunk.replace(/^[\r\n]+/, '');
    if (!s) continue;   // 하트비트 (빈 줄)
    const i = s.indexOf('\n\n');
    const lines = (i < 0 ? s : s.slice(0, i)).split('\n');
    const headers = {};
    for (const l of lines.slice(1)) {
      const k = l.indexOf(':');
      if (k > 0) headers[l.slice(0, k)] = l.slice(k + 1);
    }
    out.push({ command: lines[0], headers, body: i < 0 ? '' : s.slice(i + 2) });
  }
  return out;
}

/** SockJS 프레임 → STOMP 문자열 목록 (o 열림 · h 하트비트 · c 닫힘은 빈 목록) */
function unwrap(msg) {
  if (msg[0] === 'a') return JSON.parse(msg.slice(1));
  if (msg[0] === 'm') return [JSON.parse(msg.slice(1))];
  return [];
}

/** 태블릿의 sockjs-client 와 같은 주소: /ws/{서버}/{세션}/websocket */
function sockjsUrl() {
  const server = String(Math.floor(Math.random() * 1000)).padStart(3, '0');
  const session = Math.random().toString(36).slice(2, 12);
  return `${WS_BASE}/${server}/${session}/websocket`;
}

/** request(type, body) → Promise({ data, ms }), on(type, fn) 으로 이벤트를 받는다 */
function connect(subs, onReady, onClose) {
  const sock = new WebSocket(sockjsUrl(), null, { headers: { Origin: ORIGIN } });
  const pending = {};
  const handlers = {};
  let seq = 0;
  let openedAt = 0;
  let hb = null;
  const send = f => sock.send(JSON.stringify([f]));
  const api = {
    on(type, fn) { handlers[type] = fn; },
    request(type, body = {}) {
      const id = `r${++seq}`;
      return new Promise((resolve, reject) => {
        pending[id] = { resolve, reject, started: Date.now() };
        send(stompFrame('SEND', { destination: `/app/${type}`, 'request-id': id, 'content-type': 'application/json' }, JSON.stringify(body)));
      });
    },
    close() { sock.close(); },
  };
  sock.onopen = () => { openedAt = Date.now(); };
  sock.onmessage = e => {
    const data = String(e.data);
    if (data === 'o') {
      send(stompFrame('CONNECT', { 'accept-version': '1.2', 'heart-beat': '10000,10000' }));
      return;
    }
    if (data[0] === 'c') { cFail.add(1); console.warn('SockJS 닫힘 ' + data); return; }
    for (const raw of unwrap(data)) {
      for (const f of parseStomp(raw)) {
        if (f.command === 'CONNECTED') {
          tConnect.add(Date.now() - openedAt);
          subs.forEach((d, i) => send(stompFrame('SUBSCRIBE', { id: `s${i}`, destination: d })));
          hb = setInterval(() => send('\n'), 10_000);
          onReady(api);
        } else if (f.command === 'MESSAGE') {
          const msg = JSON.parse(f.body);
          if ('requestId' in msg) {
            const p = pending[msg.requestId];
            if (!p) continue;
            delete pending[msg.requestId];
            if (msg.ok) p.resolve({ data: msg.data, ms: Date.now() - p.started });
            else p.reject(new Error(msg.error));
          } else if (handlers[msg.type]) {
            handlers[msg.type](msg.data);
          }
        } else if (f.command === 'ERROR') {
          cFail.add(1);
          console.warn('STOMP ERROR ' + (f.headers.message || f.body));
        }
      }
    }
  };
  sock.onerror = e => { cFail.add(1); console.warn('웹소켓 오류 ' + (e.error || e)); };
  sock.onclose = () => { if (hb) clearInterval(hb); onClose(); };
  return api;
}

/* ---------------- 참여자 한 명 ---------------- */

export default function () {
  const idx = __VU;
  const who = { pin: PIN, grade: 1 + (idx % 6), classNo: 1 + (Math.floor(idx / 6) % 20), name: `${NAME}${idx}` };
  let guard = null;

  setTimeout(() => {
    const c = connect(['/user/queue/reply', '/user/queue/events', `/topic/room.${PIN}.players`], c2 => {
      let watching = false;   // 앞 화면 영상 중에는 답을 받지 않는다 → 끝나면(문제 영상은 절반에서) 답한다
      let waiting = null;     // 영상 때문에 미뤄 둔 문제
      let answered = -1;

      const answer = q => {
        if (answered === q.index) return;
        answered = q.index;
        const think = THINK_MIN + Math.random() * Math.max(0, THINK_MAX - THINK_MIN);
        setTimeout(() => {
          const value = q.type === 'short' ? '부하테스트' : q.options[Math.floor(Math.random() * q.options.length)].id;
          c2.request('player.answer', { value }).then(r => {
            tAnswer.add(r.ms);
            cAnswered.add(1);
          }).catch(e => {
            if (e.message.includes('지금은 답을 낼 수 없습니다')) {
              cLate.add(1);   // 이미 마감됨 — 실제 태블릿에서도 같은 안내가 뜬다
            } else if (e.message.includes('영상을 본 뒤에')) {
              cVideoOrder.add(1);
              cFail.add(1);
              console.error(`영상 순서 ${who.name} ${q.index + 1}번: 영상 신호보다 문제가 먼저 도착`);
              answered = -1;   // 태블릿처럼 영상이 끝나면 다시 답한다
              waiting = q;
            } else {
              cFail.add(1);
              console.error(`답 실패 ${who.name} ${q.index + 1}번: ${e.message}`);
            }
          });
        }, think);
      };
      const onQuestion = q => {
        if (watching) waiting = q;
        else answer(q);
      };

      c2.on('game.question', q => {
        if (q.index < MAX_Q) { qArrival[q.index].add(Date.now()); qCount[q.index].add(1); }
        cQuestions.add(1);
        onQuestion(q);
      });
      c2.on('game.video', v => {
        watching = v.playing;
        if (!watching && waiting) { const q = waiting; waiting = null; answer(q); }
      });
      c2.on('game.result', r => {
        if (r.index < MAX_Q) { rArrival[r.index].add(Date.now()); rCount[r.index].add(1); }
        cResults.add(1);
      });
      c2.on('game.ended', () => { cEnded.add(1); setTimeout(() => c2.close(), 15_000); });
      c2.on('game.closed', () => c2.close());
      c2.on('player.kicked', () => { cFail.add(1); c2.close(); });
      c2.on('player.replaced', () => { cFail.add(1); console.error(`${who.name}: 같은 이름으로 다른 곳에서 들어옴`); c2.close(); });

      c2.request('player.join', who).then(r => {
        tJoin.add(r.ms);
        cJoined.add(1);
        check(r.data, { '입장: 2학년_1반_이름 형식': x => x.displayName === `${who.grade}학년_${who.classNo}반_${who.name}` });
        // 문제 도중에 들어왔으면 지금 문제부터 푼다
        const s = r.data.snapshot;
        watching = s.videoPlaying;
        if (s.state === 'question' && s.question && !s.myAnswer) onQuestion(s.question);
      }).catch(e => { cFail.add(1); console.error(`입장 실패 ${who.name}: ${e.message}`); c2.close(); });
    }, () => { if (guard) clearTimeout(guard); });

    guard = setTimeout(() => c.close(), HOLD_MS);   // 방이 안 닫혀도 이 시간이 지나면 나간다
  }, Math.random() * JOIN_MS);
}

/* ---------------- 결과 ---------------- */

export function handleSummary(data) {
  // 문항별로 '첫 참여자 도착 ~ 마지막 참여자 도착' = 서버가 전원에게 보내는 데 걸린 시간
  const rows = [];
  const cell = (t, c) => (t ? `${c ? c.values.count : 0}명 · ${Math.round(t.values.max - t.values.min)}ms` : '-');
  for (let i = 0; i < MAX_Q; i++) {
    const n = i + 1;
    const q = data.metrics[`hq_arrival_q${n}`];
    const r = data.metrics[`hq_arrival_r${n}`];
    if (q || r) {
      rows.push(`  ${String(n).padStart(2)}번   문제 ${cell(q, data.metrics[`hq_count_q${n}`])}   결과 ${cell(r, data.metrics[`hq_count_r${n}`])}`);
    }
    for (const k of [`hq_arrival_q${n}`, `hq_arrival_r${n}`, `hq_count_q${n}`, `hq_count_r${n}`]) delete data.metrics[k];
  }
  const head = `\n문항별 도착 (받은 인원 · 첫 도착부터 마지막 도착까지)\n${rows.join('\n') || '  (진행된 문항 없음)'}\n`;
  return {
    stdout: textSummary(data, { indent: ' ', enableColors: true }) + head,
    [`result-${PIN}.json`]: JSON.stringify(data, null, 2),
  };
}
