// Fixtures are real frames captured from the exchange and checked by hand:
// multi-item close frames, the closed-ness flag, reverse-ordered REST rows, the
// bucket-overlap edge bar, and empty-string numerics.
import { describe, expect, it } from 'vitest';
import {
  klineStreamName,
  normalizeKlineFrame,
  parseBybitKlineRows,
  parseStreamName,
  tfToInterval,
  tickerStreamName,
  unwrapEnvelope,
} from './dialect.js';

const CFG = { exchange: 'bybit', marketType: 'spot' as const };

const item = (over: Record<string, unknown> = {}) => ({
  start: 1784367060000,
  end: 1784367119999,
  interval: '1',
  open: '63929.9',
  high: '63944.4',
  low: '63920.1',
  close: '63929.8',
  volume: '3.961', // BASE
  turnover: '253202.6', // QUOTE
  confirm: false,
  timestamp: 1784367065123,
  ...over,
});

describe('bybit dialect — WS klines', () => {
  it('normalizes a forming frame; closed ONLY via confirm (type lies)', () => {
    const out = normalizeKlineFrame(
      { topic: 'kline.1.BTCUSDT', type: 'snapshot', ts: 1, data: [item()] },
      CFG,
    )!;
    expect(out).toHaveLength(1);
    expect(out[0]!.closed).toBe(false); // type:'snapshot' must NOT mean closed
    expect(out[0]!.bar.ts).toBe(1784367060000); // ts = start, not timestamp
    expect(out[0]!.bar.v).toBeCloseTo(3.961); // BASE volume
    expect(out[0]!.topic).toEqual({
      exchange: 'bybit',
      marketType: 'spot',
      exchangeSymbol: 'BTCUSDT',
      stream: 'kline',
      tf: '1m',
    });
  });

  it('multi-item frame (closed + new forming in ONE frame — linear pattern)', () => {
    const out = normalizeKlineFrame(
      {
        topic: 'kline.1.BTCUSDT',
        type: 'snapshot',
        ts: 2,
        data: [
          item({ confirm: true }),
          item({ start: 1784367120000, confirm: false, open: '63929.8' }),
        ],
      },
      CFG,
    )!;
    expect(out).toHaveLength(2);
    expect(out[0]!.closed).toBe(true);
    expect(out[1]!.closed).toBe(false);
    expect(out[1]!.bar.ts).toBe(1784367120000);
  });

  it('structural rejects: empty-string price, future ts, non-kline frame', () => {
    expect(
      normalizeKlineFrame(
        { topic: 'kline.1.BTCUSDT', ts: 1, data: [item({ open: '' })] },
        CFG,
      ),
    ).toEqual([]);
    expect(
      normalizeKlineFrame(
        { topic: 'kline.1.BTCUSDT', ts: 1, data: [item({ start: Date.now() + 7_200_000 })] },
        CFG,
      ),
    ).toEqual([]);
    expect(normalizeKlineFrame({ topic: 'tickers.BTCUSDT', data: {} }, CFG)).toBeNull();
    expect(normalizeKlineFrame({ op: 'pong' }, CFG)).toBeNull();
  });
});

describe('bybit dialect — REST klines', () => {
  const rows = [
    ['1784367060000', '63929.9', '63944.4', '63920.1', '63929.8', '3.961', '253202.6'],
    ['1784367000000', '63925.0', '63935.0', '63910.0', '63929.9', '2.5', '159800.1'],
    ['1784366940000', '63910.0', '63930.0', '63900.0', '63925.0', '1.1', '70301.0'],
  ];

  it('reverses newest-first rows into ascending platform order', () => {
    const bars = parseBybitKlineRows(rows);
    expect(bars.map((b) => b.ts)).toEqual([1784366940000, 1784367000000, 1784367060000]);
    expect(bars[0]!.v).toBeCloseTo(1.1); // volume = BASE (turnover unused)
  });

  it('sinceMs applies STRICT ts>=start (bucket-overlap edge bar dropped)', () => {
    const bars = parseBybitKlineRows(rows, { sinceMs: 1784367000000 });
    expect(bars.map((b) => b.ts)).toEqual([1784367000000, 1784367060000]);
  });

  it('dropFormingAt drops the unfinished bar', () => {
    const bars = parseBybitKlineRows(rows, { dropFormingAt: 1784367060001 });
    expect(bars.map((b) => b.ts)).toEqual([1784366940000, 1784367000000]);
  });

  // Once the hourly and daily series began to be fetched from the exchange
  // rather than computed, the forming-bar guard mattered at every timeframe.
  // It used to be a hard-coded one minute, so an hourly page kept the hour
  // still in progress and would have stored it as a closed bar — a corrupt
  // candle that stays corrupt forever.
  describe('forming-bar guard is TF-aware', () => {
    const hourly = [
      ['1784367600000', '2', '3', '1', '2.5', '10', '0'], // 13:00, still forming
      ['1784364000000', '1', '2', '0.5', '1.5', '20', '0'], // 12:00, closed
    ];

    it('tf=1h drops a bar whose HOUR has not elapsed', () => {
      // 30 minutes into the 13:00 bucket: it is not closed yet.
      const bars = parseBybitKlineRows(hourly, { tf: '1h', dropFormingAt: 1784369400000 });
      expect(bars.map((b) => b.ts)).toEqual([1784364000000]);
    });

    it('tf=1h keeps the bar once its hour HAS elapsed', () => {
      const bars = parseBybitKlineRows(hourly, { tf: '1h', dropFormingAt: 1784371200001 });
      expect(bars.map((b) => b.ts)).toEqual([1784364000000, 1784367600000]);
    });

    it('without tf the guard stays exactly 1m — existing callers unchanged', () => {
      // Same input, same cutoff as the 1h case above: the old 60_000 rule
      // would (wrongly, for hourly data) keep the forming bar. Proving it
      // still does is what makes this fix byte-compatible for the 1m path.
      const bars = parseBybitKlineRows(hourly, { dropFormingAt: 1784369400000 });
      expect(bars.map((b) => b.ts)).toEqual([1784364000000, 1784367600000]);
    });
  });

  it('unwrapEnvelope: retCode!=0 throws with rate-limit tagging', () => {
    expect(() => unwrapEnvelope({ retCode: 10006, retMsg: 'too many' }, 'kline')).toThrow(
      /RATE-LIMIT/,
    );
    expect(unwrapEnvelope({ retCode: 0, result: { list: [1] } }, 'kline')).toEqual([1]);
  });
});

describe('bybit dialect — stream names + tf map', () => {
  it('maps platform TFs to v5 intervals (no seconds — kline1s=false)', () => {
    expect(tfToInterval('1m')).toBe('1');
    expect(tfToInterval('1h')).toBe('60');
    expect(tfToInterval('1d')).toBe('D');
    expect(tfToInterval('1s')).toBeNull();
  });

  it('builds and parses stream names round-trip', () => {
    expect(klineStreamName('BTCUSDT', '1m')).toBe('kline.1.BTCUSDT');
    expect(klineStreamName('BTCUSDT', '1s')).toBeNull();
    expect(tickerStreamName('BTCUSDT')).toBe('tickers.BTCUSDT');
    expect(parseStreamName('kline.1.BTCUSDT')).toEqual({
      kind: 'kline',
      exchangeSymbolLower: 'btcusdt',
    });
    expect(parseStreamName('tickers.BTCUSDT')).toEqual({
      kind: 'ticker',
      exchangeSymbolLower: 'btcusdt',
    });
    expect(parseStreamName('orderbook.50.BTCUSDT')).toEqual({ kind: 'other' });
  });
});
