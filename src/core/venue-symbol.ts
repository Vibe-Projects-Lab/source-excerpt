// What a listing looks like once it stops being exchange-shaped.
//
// Producing this from an exchange's own catalogue is adapter work; everything
// downstream sees only this.
//
// One rule about it is worth stating: metadata added to this shape applies to
// every market of an exchange by default, spot and perpetual alike. The parse
// is shared per exchange and every domain runs the same sync, so a new field
// lands on both markets for free — which also means testing it has to cover
// both.
import type { InstrumentInfo } from './instruments.js';

export interface VenueSymbol {
  symbol: string; // exchange_symbol as the venue reports it
  base: string; // venue's own base-asset id
  quote: string;
  /** Venue status mapped to the instruments enum ('active' | 'halted'). */
  status: 'active' | 'halted';
  contractAddress: string | null;
  network: string | null;
  /** Venue-reported listing time, when the venue has one. */
  listedAt: Date | null;
  /** Normalized exchange metadata destined for
   *  instrument_metadata.instrument_info (tick_size + trading rules) —
   *  {} when the venue reports nothing. Shape owned by shared/instruments.ts. */
  info: InstrumentInfo;
}
