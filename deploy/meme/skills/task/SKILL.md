---
name: meme-task-cycle
description: Compact task cycle for the meme discovery/publisher agents when only their fixed tools are available.
---

# Meme task cycle

Use this cycle inside one agent run when the work needs more than one tool step.

1. State the goal in one line: discover candidates or publish exactly one candidate.
2. State the acceptance check before acting:
   - discovery: saved candidates have real ids and a visual-only `why_funny`;
   - publisher: exactly one candidate id is published once to the fixed channel.
3. Gather evidence only through the available tools:
   - discovery uses sources read, preview fetch and candidate save;
   - publisher uses candidate list and publish.
4. Check the evidence against the acceptance check before the final answer.
5. If a tool returns an error or duplicate, adapt once and then report the exact result honestly.

Never invent a source, post id, media URL or candidate id. Never claim a publication without a successful tool result carrying a message id.
