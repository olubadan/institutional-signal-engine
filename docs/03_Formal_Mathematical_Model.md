# Real-Time Institutional Signal Engine — Formal Specification

This is the authoritative owner-supplied specification. Wording, equations,
constants, and ordering are preserved; only Markdown line breaks were normalized
from the source document.

---

### 1. State Space

At time $t$, for each candidate symbol $i \in \mathcal{U}$ (self-selected liquid universe):

$$
x_i(t) = \big[\, p_i(t),\ v_i(t),\ \text{OI}_i(t),\ C_i(t),\ \sigma_i(t),\ d_i(t),\ m(t),\ s_i(t) \,\big]
$$

where $p_i$ = price, $v_i$ = volume, $\text{OI}_i$ = open interest, $C_i$ = net call premium, $\sigma_i$ = spread, $d_i$ = distance to resistance, $m(t)$ = market index state, $s_i(t)$ = sector index state.

---

### 2. Indicator Functions

$$
\mathbb{1}_{\text{liq}}(i) = \mathbb{1}\big[v_i(t) \geq V_{\min} \ \wedge\ \sigma_i(t) \leq \sigma_{\max}\big]
$$

$$
\mathbb{1}_{\text{opt}}(i,t) = \mathbb{1}\big[C_i(t) \geq C_{\min} \ \wedge\ \tfrac{v_i^{\text{opt}}(t)}{\text{OI}_i(t)} \geq \rho_{\min}\big]
$$

$$
\mathbb{1}_{\text{eq}}(i,t) = \mathbb{1}\big[\Delta p_i(t) > 0\big], \qquad \mathbb{1}_{\text{mkt}}(t) = \mathbb{1}\big[\Delta m(t) > 0\big], \qquad \mathbb{1}_{\text{sec}}(i,t) = \mathbb{1}\big[\Delta s_i(t) > 0\big]
$$

---

### 3. Signal Validity — S

$$
S(i,t) = \mathbb{1}_{\text{liq}}(i) \wedge \mathbb{1}_{\text{opt}}(i,t) \wedge \mathbb{1}_{\text{eq}}(i,t) \wedge \mathbb{1}_{\text{mkt}}(t) \wedge \mathbb{1}_{\text{sec}}(i,t) \ \in \{0,1\}
$$

---

### 4. Freshness — F

Let $t_0(i)$ = first time $S(i,\cdot) = 1$ in current episode. Define decay function $\delta(t - t_0)$, monotonically non-increasing:

$$
F(i,t) = \mathbb{1}\big[\delta(t - t_0(i)) \geq \delta_{\min}\big] \wedge \mathbb{1}\big[\mathbb{1}_{\text{opt}}(i,t) = 1\big] \wedge \mathbb{1}\big[\mathbb{1}_{\text{eq}}(i,t) = 1\big]
$$

---

### 5. Room — R

$$
R(i,t) = \mathbb{1}\big[d_i(t) \geq \tau_{\text{TP}}\big] \wedge \mathbb{1}\big[v_i(t) \geq V_{\min}\big] \wedge \mathbb{1}\big[\sigma_i(t) \leq \sigma_{\max}\big] \wedge \mathbb{1}\big[K(t) < K_{\max}\big]
$$

where $K(t)$ = concurrent open positions, $K_{\max}$ = capacity ceiling.

---

### 6. Executability Gate

$$
E(i,t) = S(i,t) \wedge F(i,t) \wedge R(i,t) \ \in \{0,1\}
$$

$$
\mathcal{C}(t) = \{\, i \in \mathcal{U} \ : \ E(i,t) = 1 \,\}
$$

---

### 7. Candidate Selection — Lexicographic Ordering

$$
i^{*}(t) = \operatorname*{arg\,lex\,max}_{i \in \mathcal{C}(t)} \Big(\, \delta(t-t_0(i)),\ C_i(t),\ \tfrac{v_i^{\text{opt}}(t)}{\text{OI}_i(t)},\ \text{RVOL}_i(t),\ d_i(t),\ -\text{ord}(i) \,\Big)
$$

$$
\text{Fire}(t) = \mathbb{1}\big[\mathcal{C}(t) \neq \varnothing\big]
$$

---

### 8. Decision Rule

$$
a(t) = \begin{cases} \text{EXECUTE}(i^{*}(t)) & \text{if } \text{Fire}(t) = 1 \\[4pt] \varnothing & \text{if } \text{Fire}(t) = 0 \end{cases}
$$

---

### 9. Order Execution

Entry:

$$
p_{\text{entry}} = \min\big(\text{ask}_i(t),\ p_i(t)(1+\ell)\big), \qquad \ell = 0.005
$$

Bracket:

$$
p_{\text{TP}} = p_{\text{entry}}(1+\tau_{\text{TP}}), \qquad \tau_{\text{TP}} = 0.0025
$$

Position size:

$$
n_i = \frac{B}{p_{\text{entry}}}, \qquad B = \text{configured dollar allocation}
$$

Exit condition (fixed horizon $T$, no leverage):

$$
p_{\text{exit}} = \begin{cases} p_{\text{TP}} & \text{if } \exists\, t' \in (t, t+T]:\ p_i(t') \geq p_{\text{TP}} \\[4pt] p_i(t+T) & \text{if } F(i,t') = 0 \text{ for some } t' \leq t+T \\[4pt] p_i(t+T) & \text{otherwise} \end{cases}
$$

---

### 10. Per-Trade Return

$$
r = \frac{p_{\text{exit}} - p_{\text{entry}}}{p_{\text{entry}}} - \kappa, \qquad \kappa = \text{execution cost (spread + fee)}
$$

---

### 11. Capital Recycling — Little's Law

$$
L = \lambda W
$$

$L$ = capital in process (fixed), $\lambda$ = fire rate (trades/day), $W$ = mean dwell time.

$$
\lambda(t) = \frac{L}{W(t)}, \qquad \frac{d\lambda}{dW} < 0
$$

---

### 12. Daily Expected Return

$$
\mathbb{E}[R_{\text{day}}] = N \cdot \big(p_{\text{win}} \cdot \tau_{\text{TP}} - (1-p_{\text{win}}) \cdot \bar{\ell}_{\text{loss}} - \kappa\big)
$$

$$
N = \lambda \cdot T_{\text{session}}, \qquad p_{\text{win}} = \Pr\big[p_{\text{exit}} = p_{\text{TP}} \mid E(i,t)=1\big]
$$

---

### 13. Break-even Condition

$$
p_{\text{win}}^{*} = \frac{\bar{\ell}_{\text{loss}} + \kappa}{\tau_{\text{TP}} + \bar{\ell}_{\text{loss}}}
$$

$$
\mathbb{E}[R_{\text{day}}] > 0 \iff p_{\text{win}} > p_{\text{win}}^{*}
$$

---

### 14. System Invariant

$$
\forall t:\quad a(t) \neq \varnothing \implies S(i^*,t) \wedge F(i^*,t) \wedge R(i^*,t) = 1
$$

$$
\varnothing \text{ is the unique fixed point when } \mathcal{C}(t) = \varnothing
$$
