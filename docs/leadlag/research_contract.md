# Lead/Lag Measurement Foundation

This is an offline measurement foundation for the existing observation-only
options-to-equity system. Its North Star is to determine, without assuming the
answer, whether meaningful call-option pressure occurs before an abnormal
response in the underlying equity and to measure the lag.

The research keeps three questions separate:

1. **Structural:** does qualifying call-option pressure precede an abnormal
   equity response?
2. **Temporal:** when did option pressure begin, when did the equity response
   begin, and what is the lag under the available timestamp provenance?
3. **Economic (later):** after legitimate detection and realistic execution
   delay, how much favorable movement remains and how often might approximately
   25bp remain?

Valid outcomes include `OPTIONS_FIRST`, `EQUITY_FIRST`, `SIMULTANEOUS`,
`NO_RESPONSE`, `PRE_EXISTING_EQUITY_MOVE`, and `AMBIGUOUS`. The laboratory and
its results are synthetic evidence only; they cannot establish a live-market
claim.

All analysis is anti-look-ahead: onset is not replaced by the time at which a
cluster qualified or was emitted. Existing CONTROL and SHADOW definitions are
unchanged. This work is observation-only; trading remains disabled and no
orders are constructed or submitted. The contract is designed to falsify the
hypothesis as well as support it.
