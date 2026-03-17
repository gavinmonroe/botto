# Harness Evolution Summary

Running log of prompt evolution rounds.

---

## Round 1

- **Winner:** `v001` (score: 87.3)
- **Parent:** `v000` (delta: +0.0)
- **Pass rate:** 2/2
- **Improved:** false
- **Learnings:** v001 is the stronger baseline to build on — it wins consistently on score, iterations, and time across all test cases.; Iteration counts (15–24) are the primary bottleneck; prompts that help the model converge faster on the correct fix will yield the biggest score improvements.; The performance gap between variants widens on the easier test case (gl-0002), suggesting that prompt clarity has more impact when the problem is tractable and the model just needs better direction, not more capability.; Both variants leave ~10-15 points of headroom in score, so there's meaningful room for improvement beyond this baseline round.; With only 2 test cases, variance is high — future rounds should weight consistency across cases heavily when comparing variants, not just mean score.

---

## Round 2

- **Winner:** `v002` (score: 90.9)
- **Parent:** `v000` (delta: +2.8)
- **Pass rate:** 2/2
- **Improved:** true
- **Learnings:** v002's improvements come primarily from faster convergence (14.5 vs 19.0 mean iterations), confirming that iteration count remains the dominant lever for score improvement in this system.; Prompts that enable adaptive convergence — solving easier problems faster rather than following a fixed iteration pattern — are a marker of higher quality; v002 shows this property while v001 does not.; The round 1 finding holds and strengthens: prompt clarity has outsized impact on tractable problems (gl-0002 gap is 3.7 points vs gl-0001's 1.9 points), so optimizing for "fast path" resolution on straightforward fixes is high-value.; We've moved from 87.3 → 90.9 across two rounds (+3.6 points), but the remaining ~9 points will be harder to capture — future variants should target specific failure modes (e.g., unnecessary iteration cycles, misdiagnosis of root cause) rather than broad structural changes.; v001's identical performance across both test cases (19 iters, 88.1 score on each) suggests it may contain overly rigid or prescriptive instructions that prevent the model from taking shortcuts when the fix is obvious — future prompts should avoid over-constraining the fix process.

---

