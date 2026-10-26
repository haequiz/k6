// haequiz 실서버 동시 부하 테스트 — 참여자 N명(기본 200)이 '같은 순간에' 입장하고, 문제마다 '같은 순간에' 답한다.
// quiz-load.js 는 입장을 30초에, 답을 1~6초에 나눠 보냈다. 이 스크립트는 전원이 한꺼번에 몰리는 최악의 경우를 본다.
//   1. 시작하자마자 N명이 한꺼번에 연결한다 (태블릿과 같은 /ws/info → SockJS websocket → STOMP · 10초 하트비트)
//   2. 시작 JOIN_AT_SEC(기본 10초) 뒤 같은 시각에 전원이 입장 요청을 보낸다
//   3. 문제가 오면(영상 문제는 답 받기가 열리면) 정확히 ANSWER_SEC(기본 3초) 뒤에 전원이 답한다
//   4. 태블릿처럼 타이틀 · 배경 · 문제 · 보기 이미지를 받는다 (같은 이미지는 한 사람이 한 번만 = 브라우저 캐시)
// 응답은 태블릿(realtime.ts)처럼 15초까지 기다린다. 넘으면 '응답 없음'으로 세고, 입장은 학생처럼 한 번 더 누른다.
// 방 만들기 · 진행(시작 · 마감 · 다음 문제 · 결과 발표)은 운영자가 진행 화면에서 직접 한다.
//
// 실행: k6 run -e PIN=123456 burst-load.js
import http from 'k6/http';
import exec from 'k6/execution';
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
const JOIN_AT_MS = Number(__ENV.JOIN_AT_SEC || 10) * 1000;   // 시작 후 이 시각에 전원이 동시에 입장
const ANSWER_MS = Number(__ENV.ANSWER_SEC || 3) * 1000;      // 문제를 받고 정확히 이만큼 뒤에 전원이 답한다
const MEDIA = __ENV.MEDIA !== '0';                           // 0 이면 이미지를 받지 않는다
const TIMEOUT_MS = 15_000;                                    // 태블릿과 같은 응답 대기 시간
const HOLD_MS = Number(__ENV.HOLD_MIN || 60) * 60_000;
const NAME = __ENV.NAME_PREFIX || '동시';                      // 같은 방에서 다시 돌릴 때는 바꾼다
const MAX_Q = 50;

if (!/^\d{6}$/.test(PIN)) throw new Error('PIN 6자리를 넣어 주세요: k6 run -e PIN=123456 burst-load.js');

const tConnect = new Trend('hq_connect_ms', true);   // 웹소켓 열림 → STOMP 연결 완료
const tJoin = new Trend('hq_join_ms', true);         // 입장 요청 → 응답
const tAnswer = new Trend('hq_answer_ms', true);     // 답 제출 → 응답
const tMedia = new Trend('hq_media_ms', true);       // 이미지 한 장 받기
const cJoined = new Counter('hq_joined');
const cAnswered = new Counter('hq_answered');
const cQuestions = new Counter('hq_questions_received');
const cResults = new Counter('hq_results_received');
const cEnded = new Counter('hq_ended_received');
const cFail = new Counter('hq_fail');                // 응답 실패 · STOMP ERROR · 연결 오류
const cJoinLost = new Counter('hq_join_lost');       // 입장 응답이 15초 안에 안 옴 = 태블릿에 '서버 응답이 없습니다'
const cAnswerLost = new Counter('hq_answer_lost');   // 답 응답이 15초 안에 안 옴
const cMediaFail = new Counter('hq_media_fail');     // 이미지 못 받음 (태블릿에서 이미지가 깨진다)
const cLate = new Counter('hq_late_answers');        // 진행자가 먼저 마감해서 늦게 낸 답 (실패 아님)
const cVideoOrder = new Counter('hq_video_order');   // 영상 신호보다 문제가 먼저 와서 잠긴 문제에 답함
// 보낸 · 받은 시각(이 PC 시계). 끝나고 '첫 ~ 마지막' 차이로 얼마나 동시에 몰렸는지, 전원이 받기까지 얼마나 걸렸는지 본다
const joinSent = new Trend('hq_sent_join');
const qArrival = Array.from({ length: MAX_Q }, (_, i) => new Trend(`hq_arrival_q${i + 1}`));
const aSent = Array.from({ length: MAX_Q }, (_, i) => new Trend(`hq_sent_a${i + 1}`));
const rArrival = Array.from({ length: MAX_Q }, (_, i) => new Trend(`hq_arrival_r${i + 1}`));

export const options = {
  scenarios: {
    players: { executor: 'per-vu-iterations', vus: PLAYERS, iterations: 1, maxDuration: `${HOLD_MS + JOIN_AT_MS + 60_000}ms` },
  },
  summaryTrendStats: ['count', 'avg', 'min', 'med', 'max', 'p(90)', 'p(95)'],   // count: 요약에 인원 수를 쓴다
  thresholds: {
    hq_join_ms: ['p(95)<1000'],
    hq_answer_ms: ['p(95)<500'],
    hq_fail: ['count==0'],
    hq_join_lost: ['count==0'],
    hq_answer_lost: ['count==0'],
    hq_media_fail: ['count==0'],
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

/** request(type, body) → Promise({ data, ms }). 15초 안에 응답이 없으면 lost 오류. on(type, fn) 으로 이벤트를 받는다 */
function connect(subs, onReady, onClose) {
  const sock = new WebSocket(sockjsUrl(), null, { headers: { Origin: ORIGIN } });
  const pending = {};
  const handlers = {};
  let seq = 0;
  let subSeq = 0;
  let openedAt = 0;
  let hb = null;
  const send = f => { if (sock.readyState === 1) sock.send(JSON.stringify([f])); };   // 1 = OPEN. 닫히는 중에 보내면 오류
  const subscribe = d => send(stompFrame('SUBSCRIBE', { id: `s${subSeq++}`, destination: d }));
  const api = {
    on(type, fn) { handlers[type] = fn; },
    subscribe,
    request(type, body = {}) {
      const id = `r${++seq}`;
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          delete pending[id];
          const e = new Error('서버 응답이 없습니다');
          e.lost = true;
          reject(e);
        }, TIMEOUT_MS);
        pending[id] = { resolve, reject, timer, started: Date.now() };
        send(stompFrame('SEND', { destination: `/app/${type}`, 'request-id': id, 'content-type': 'application/json' }, JSON.stringify(body)));
      });
    },
    close() { if (hb) clearInterval(hb); hb = null; sock.close(); },
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
          subs.forEach(subscribe);
          hb = setInterval(() => send('\n'), 10_000);
          onReady(api);
        } else if (f.command === 'MESSAGE') {
          const msg = JSON.parse(f.body);
          if ('requestId' in msg) {
            const p = pending[msg.requestId];
            if (!p) continue;
            delete pending[msg.requestId];
            clearTimeout(p.timer);
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
  sock.onclose = () => {
    if (hb) clearInterval(hb);
    for (const id in pending) clearTimeout(pending[id].timer);   // 닫힌 연결의 응답은 더 기다리지 않는다
    onClose();
  };
  return api;
}

/* ---------------- 이미지 (태블릿 <img> · 배경) ---------------- */

const seen = new Set();   // VU(참여자)마다 따로 = 태블릿 브라우저 캐시

function fetchMedia(path, who) {
  if (!MEDIA || !path || seen.has(path)) return;
  seen.add(path);
  // 프론트 mediaUrl() 과 같다: /media/… 는 백엔드, 그 밖의 / 경로는 프론트 주소 기준
  const url = path.startsWith('/media/') ? API + path : path.startsWith('/') ? ORIGIN + path : path;
  http.asyncRequest('GET', url, null, { responseType: 'none', timeout: '60s', tags: { name: 'media' } }).then(r => {
    if (r.status === 200 || r.status === 206) {
      tMedia.add(r.timings.duration);
    } else {
      cMediaFail.add(1);
      console.error(`이미지 실패 ${who} ${r.status} ${r.error || ''} ${path}`);
    }
  });
}

function fetchQuestionMedia(q, who) {
  fetchMedia(q.image, who);
  (q.options || []).forEach(o => fetchMedia(o.image, who));
}

/* ---------------- 참여자 한 명 ---------------- */

export default function () {
  const idx = __VU;
  const who = { pin: PIN, grade: 1 + (idx % 6), classNo: 1 + (Math.floor(idx / 6) % 20), name: `${NAME}${idx}` };
  let guard = null;

  // sockjs-client 는 웹소켓을 열기 전에 /ws/info 를 먼저 받는다
  const info = http.get(`${API}/ws/info?t=${Date.now()}`, { headers: { Origin: ORIGIN }, tags: { name: 'ws-info' } });
  if (info.status !== 200) { cFail.add(1); console.error(`/ws/info 실패 ${who.name} ${info.status}`); }

  const c = connect(['/user/queue/reply', '/user/queue/events'], c2 => {
    let watching = false;   // 앞 화면 영상 중에는 답을 받지 않는다 → 끝나면(문제 영상은 절반에서) 답한다
    let waiting = null;     // 영상 때문에 미뤄 둔 문제
    let answered = -1;

    const answer = q => {
      if (answered === q.index) return;
      answered = q.index;
      setTimeout(() => {
        if (q.index < MAX_Q) aSent[q.index].add(Date.now());
        const value = q.type === 'short' ? '부하테스트' : q.options[Math.floor(Math.random() * q.options.length)].id;
        c2.request('player.answer', { value }).then(r => {
          tAnswer.add(r.ms);
          cAnswered.add(1);
        }).catch(e => {
          if (e.lost) {
            cAnswerLost.add(1);
            console.error(`답 응답 없음 ${who.name} ${q.index + 1}번 (15초)`);
          } else if (e.message.includes('지금은 답을 낼 수 없습니다')) {
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
      }, ANSWER_MS);
    };
    const onQuestion = q => {
      fetchQuestionMedia(q, who.name);
      if (watching) waiting = q;
      else answer(q);
    };

    c2.on('game.question', q => {
      if (q.index < MAX_Q) qArrival[q.index].add(Date.now());
      cQuestions.add(1);
      onQuestion(q);
    });
    c2.on('game.video', v => {
      watching = v.playing;
      if (!watching && waiting) { const q = waiting; waiting = null; answer(q); }
    });
    c2.on('game.result', r => {
      if (r.index < MAX_Q) rArrival[r.index].add(Date.now());
      cResults.add(1);
    });
    c2.on('game.ended', () => { cEnded.add(1); setTimeout(() => c2.close(), 15_000); });
    c2.on('game.closed', () => c2.close());
    c2.on('player.kicked', () => { cFail.add(1); c2.close(); });
    c2.on('player.replaced', () => { cFail.add(1); console.error(`${who.name}: 같은 이름으로 다른 곳에서 들어옴`); c2.close(); });

    const join = again => {
      c2.request('player.join', who).then(r => {
        tJoin.add(r.ms);
        cJoined.add(1);
        check(r.data, { '입장: 2학년_1반_이름 형식': x => x.displayName === `${who.grade}학년_${who.classNo}반_${who.name}` });
        c2.subscribe(`/topic/room.${PIN}.players`);   // 태블릿처럼 입장한 뒤에 문제 토픽을 구독한다
        fetchMedia(r.data.quiz.titleImage, who.name);
        fetchMedia(r.data.quiz.backgroundImage, who.name);
        // 문제 도중에 들어왔으면 지금 문제부터 푼다
        const s = r.data.snapshot;
        watching = s.videoPlaying;
        if (s.state === 'question' && s.question && !s.myAnswer) onQuestion(s.question);
      }).catch(e => {
        if (e.lost && !again) {
          cJoinLost.add(1);   // 학생은 15초 멈춘 뒤 '서버 응답이 없습니다'를 보고 다시 누른다
          console.error(`입장 응답 없음 ${who.name} (15초) — 한 번 더 누름`);
          join(true);
          return;
        }
        cFail.add(1);
        console.error(`입장 실패 ${who.name}: ${e.message}`);
        c2.close();
      });
    };
    // 모든 참여자가 같은 시각에 입장 버튼을 누른다 (그때까지 연결 못 한 사람은 연결되는 대로)
    setTimeout(() => {
      joinSent.add(Date.now());
      join(false);
    }, Math.max(0, exec.scenario.startTime + JOIN_AT_MS - Date.now()));
  }, () => { if (guard) clearTimeout(guard); });

  guard = setTimeout(() => c.close(), HOLD_MS);   // 방이 안 닫혀도 이 시간이 지나면 나간다
}

/* ---------------- 결과 ---------------- */

export function handleSummary(data) {
  // '첫 번째 ~ 마지막' 시각 차이: 보낸 쪽은 얼마나 한꺼번에 몰렸는지, 받은 쪽은 서버가 전원에게 보내는 데 걸린 시간
  const cell = name => {
    const t = data.metrics[name];
    return t ? `${t.values.count}명 · ${Math.round(t.values.max - t.values.min)}ms` : '-';
  };
  const rows = [];
  for (let i = 0; i < MAX_Q; i++) {
    const n = i + 1;
    if (data.metrics[`hq_arrival_q${n}`] || data.metrics[`hq_arrival_r${n}`]) {
      rows.push(`  ${String(n).padStart(2)}번   문제 도착 ${cell(`hq_arrival_q${n}`)}   답 전송 ${cell(`hq_sent_a${n}`)}   결과 도착 ${cell(`hq_arrival_r${n}`)}`);
    }
    for (const k of [`hq_arrival_q${n}`, `hq_sent_a${n}`, `hq_arrival_r${n}`]) delete data.metrics[k];
  }
  const j = data.metrics.hq_sent_join;
  delete data.metrics.hq_sent_join;
  const head = `\n동시 입장: ${j ? `${j.values.count}명이 ${Math.round(j.values.max - j.values.min)}ms 안에 입장 요청` : '(입장 요청 없음)'}\n`
    + `\n문항별 (인원 · 첫 번째부터 마지막까지 걸린 시간)\n${rows.join('\n') || '  (진행된 문항 없음)'}\n`;
  return {
    stdout: textSummary(data, { indent: ' ', enableColors: true }) + head,
    [`result-burst-${PIN}.json`]: JSON.stringify(data, null, 2),
  };
}
