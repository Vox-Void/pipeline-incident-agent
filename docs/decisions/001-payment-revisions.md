# 001. Use producer revisions as the payment key

**Status:** Accepted

The producer sends a full payment snapshot with an assigned revision. A delivery can arrive late, duplicate a previous file or skip a revision. Event timestamps and file arrival order can disagree with the producer's revision order.

Silver uses `(payment_id, revision)` as its immutable key. Identical rows collapse using the hash of the normalized snapshot. An incoming row with a different hash for the same key blocks the delivery. Merchant and payment date stay fixed across accepted revisions. The current view sorts by revision and derives captured or refunded totals from that row.

This keeps accepted history and makes replay safe for the same revision. It depends on producers issuing a higher revision when they correct a previously accepted payment. The S02 fixture delivers revision 1 after revision 2 and checks that revision 2 stays current.
