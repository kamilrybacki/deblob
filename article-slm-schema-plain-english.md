# Can a Tiny AI Babysit Your Data? What We Learned Letting a Small Language Model Loose on Messy Records

*A plain-English write-up of a joint research project (Claude + Hermes) on using small language models to understand incoming data and keep its "shape" under control.*

---

## The problem nobody sees until it breaks

Almost every piece of software you use is quietly passing little packages of data around behind the scenes. An order gets placed, and a package goes out that says *"order number 4471, amount 39.99, currency USD."* Something downstream receives that package, trusts it looks a certain way, and files it into a database, a spreadsheet, a report.

The unwritten agreement about what that package looks like — which fields exist, what type each one is, what they mean — is called a **schema**. You can think of a schema as the shape of a jigsaw puzzle piece. As long as every new piece has the same shape, everything slots together and nobody notices the schema is even there.

The trouble starts when the shape changes. A developer on some other team renames `amount` to `total`, or starts sending the price in *cents* (3999) instead of *dollars* (39.99), or wraps everything in a new layer. The package still looks perfectly valid — it's still a tidy little piece — but it no longer fits. And because it *looks* fine, the breakage is often silent. Reports quietly go wrong. Numbers are off by a factor of 100. Sometimes for months.

The dream is to have something smart sit in the middle, watch the data flow by, notice when the shape drifts, and figure out what everything means — automatically. And these days, "something smart" makes people think: *let's use AI.*

That's the idea we set out to pressure-test.

## The tempting shortcut, and why it bites

The specific flavor of AI we looked at is a **Small Language Model**, or SLM. It's the same basic technology as ChatGPT, but shrunk down small enough to run privately on an ordinary computer — no giant data center, no sending your data off to anyone. That privacy and low cost is exactly why it's appealing for babysitting a company's internal data.

So the tempting shortcut is: *point the little AI at the incoming data and let it decide everything — what each field means, whether the shape changed, how to fix it.*

Here's the catch, and it's the single most important thing we found:

> **A small AI is a confident guesser that never says "I don't know."**

Imagine hiring a brand-new intern who is eager, fast, and — crucially — physically incapable of admitting uncertainty. Hand them a mystery field called `cust_seg` with no explanation, and they won't shrug and ask. They'll cheerfully write down *"customer segment,"* which might be right… or it might actually be a risk category, or an internal account tier. It sounds plausible. It's grammatically perfect. It passes every sanity check you didn't specifically design to catch it. And it's wrong.

The real-world version we kept hitting: a field called `amount`. Is `3999` thirty-nine dollars and ninety-nine cents, or three thousand nine hundred and ninety-nine dollars? To a human with context, obvious. To the little AI staring at a lone number, it's a coin flip dressed up as a confident answer. Get that wrong and every financial figure downstream is off by 100×.

The lesson isn't "AI is useless here." It's that **an eager guesser is fantastic at some jobs and catastrophic at others** — and the entire art is knowing which is which.

## The one idea that makes it safe: separate the jobs

The breakthrough in our research (this was Hermes' framing, and it turned out to be the spine of the whole report) is to stop treating "understanding the data" as one job. It's actually **three jobs**, and they must never be blended:

**1. Establishing facts.** *What is literally in this package?* Which fields exist, are they numbers or text, has the shape changed since last time? This is boring, mechanical, and — importantly — it has exactly one right answer every time. A plain old computer program does this perfectly and identically on every run. **No AI needed, and no AI wanted.**

**2. Making guesses.** *What might this stuff mean?* Is `cust_no` the same thing as `customer_id`? What's a good human-readable name for this new kind of record? This is where meaning and language live — and this is the *only* place the little AI belongs.

**3. Making decisions.** *Do we act on the guess?* Do we officially update the agreed-upon shape, merge two things together, or convert dollars to cents? These are the consequential, hard-to-undo actions. They go back to the boring, predictable computer program — and for the genuinely ambiguous calls, to a **human** who clicks "approve."

Picture a courtroom. The AI is a **witness** — it can offer testimony and point out things that seem relevant. It is emphatically **not the judge** and **not the court clerk who files the official record.** A witness who started forging court documents would be a disaster. The whole design is about keeping the witness in the witness box.

There's a slogan that captures it, which both halves of our research arrived at independently:

> **Use the AI to compress confusion into a suggestion. Use plain, predictable software to establish facts and make the decisions.**

## What actually happened when we tried it

We tested this on a real homelab system (nicknamed **Deblob**) that watches dozens of live data streams. A few moments from the trenches:

**The AI didn't catch the problem — it just described it.** When a data stream's shape drifted, the thing that actually *noticed* was the dumb, reliable comparison: *last time this shape had fingerprint A, now it's fingerprint B.* The little AI's job was only to translate that into English a human could act on: *"the price field moved and turned into text; it might now be in cents."* Useful! But if we'd trusted the AI to be the one keeping watch, it would have missed things and hallucinated others. The **boring watchman caught the burglar; the AI wrote the incident report.**

**Small model, surprisingly un-small appetite.** One night the AI fell over with an out-of-memory crash — a real outage. The instinct is to blame one setting. The truth was messier and more useful: a tiny model can still consume a lot of memory once you account for how much text it's chewing on, how many requests are queued, and hidden caches. The fix wasn't one knob; it was putting a **firm ceiling on every dimension at once** — how many requests at a time, how long the queue, how much scratch memory. After that: 421 requests over 12 minutes, zero crashes. The takeaway for anyone deploying one of these: **treat the AI's resource limits as a safety feature, not an afterthought.**

**"Looks valid" is not "is correct."** You can force the little AI to always produce perfectly-formatted output — right fields, right types, no gibberish. That's genuinely worth doing. But it's worth being clear-eyed about what it buys you: it guarantees the *envelope* is well-formed, not that the *letter inside* is true. As we put it in the report: **it validates the transport, not the truth.**

**Let it say "I don't know."** The most valuable upgrade you can give one of these models isn't making it smarter — it's teaching it to **abstain**. A guesser that hands off the hard cases to a human, instead of bluffing, is worth far more than one that's slightly more accurate but always answers.

## So — when is it actually worth using an SLM?

Our honest bottom line, translated out of the jargon:

**Good jobs for the little AI:**
- Coming up with a sensible human-readable name for a new kind of record.
- Spotting that two differently-named fields (`cust_no`, `buyer_id`) probably mean the same thing — *as a suggestion for a human to confirm.*
- Picking the best match out of a short list of known options.
- Explaining, in plain language, a change that a plain program already detected.

**Overkill — don't bother:**
- Anything with one exact right answer (is this a number or text? did the shape change?). Plain code is faster, free, and never wrong.
- Anything that happens on every single record in a steady stream. You call the AI once, when something genuinely *new* and confusing shows up — not millions of times for routine traffic.

**Genuinely dangerous — never let it decide alone:**
- Officially changing the agreed-upon shape of the data.
- Merging two things together, or converting units (dollars↔cents, seconds↔milliseconds).
- Anything touching personal or sensitive information.
- Anything you can't easily undo.

For every item on that last list, a plain rulebook makes the call, and a human signs off on the truly ambiguous ones.

## The takeaway

Small AI models are a real, useful new tool for making sense of messy data — but not in the way the hype suggests. They are not a magic box you point at chaos to get order. They're a fast, private, tireless **assistant that's brilliant at suggesting meanings and terrible at knowing its own limits.**

Give it the one job it's good at — turning confusion into a clearly-labeled suggestion — and wrap that suggestion in old-fashioned, predictable software that checks the facts and makes the decisions. Do that, and you get the best of both worlds: a system that's genuinely smart about *language and meaning*, and genuinely trustworthy about *facts and consequences.*

Or, in one sentence: **let the AI be the witness, never the judge.**

---

*This article is a plain-language summary of a joint research report, "SLMs for Dynamic Heterogeneous-Data Identification & Schema Control" (run jr-slm-schema-241439), produced by two AI research agents working in parallel and grounded in a real running system plus 30+ external sources. The technical version has the citations, numbers, and engineering detail.*
