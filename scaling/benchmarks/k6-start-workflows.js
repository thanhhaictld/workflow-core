import http from 'k6/http';
import { check } from 'k6';

const url = __ENV.URL || 'http://localhost:5180';
export const options = {
  scenarios: {
    workflow_starts: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || '100'),
      timeUnit: '1s',
      duration: __ENV.DURATION || '5m',
      preAllocatedVUs: Number(__ENV.VUS || '100'),
      maxVUs: Number(__ENV.MAX_VUS || '800'),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<1000'],
  },
};

export default function () {
  const token = `k6-${__VU}-${__ITER}-${__ENV.RUN_ID || 'local'}`;
  const response = http.post(`${url}/api/workflows`, JSON.stringify({
    tenantId: token,
  }), {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': token,
    },
    timeout: '30s',
  });
  check(response, { 'workflow accepted': r => r.status === 202 });
}
