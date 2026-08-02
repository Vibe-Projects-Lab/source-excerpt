// Venue-NEUTRAL listing shape, consumed by the coin registry. Producing it from
// a venue's own instrument-info dialect is adapter work — see the venue's
// exchange-info module for the other side of this boundary.
//
// A standing rule about this shape: venue metadata added here applies to ALL
// market types of a venue by default, spot and perpetual alike, never
// spot-only. The dialect parse is shared per venue and every API domain runs
// the same sync, so a new field lands on both markets for free — which also
// means verification of such a feature has to cover both.
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
  /** + the backlog: NORMALIZED venue metadata destined for
   *  instrument_metadata.instrument_info (tick_size + trading rules) —
   *  {} when the venue reports nothing. Shape owned by shared/instruments.ts. */
  info: InstrumentInfo;
}
