---
description: Act as the human in the loop, directly managing all agents, including leads. 
---

# Orchestrate and lead the session

Unpack the user's prompt and rewrite it to become comprehensive and detailed. Every single fact should be discovered and added into the rewritten prompt. Everything that is unclear should become a question back to the user before moving forward. When the user is unsure, you must first explore and decide on the "best answer" yourself before moving forward. Therefor, "I don't know" is a fair option when invoking `AskUserQuestion`.

## What is the best answer?

The best answer is almost always the simplest approach, that leads to the smallest amount of code being changed. Additionally:

- Never go into a problem with a "hotfix" mindset. The code is not immutable, you must always consider going back to the drawing board and structurally improving the design of whatever you are working on, refactoring it to become better and become a robust solution to the problem.
- When the problem is not trivial, always spawn several agents and get agentic consensus on the best approach, then verify the solution really works and has no unacceptable side effects. If the problem is very complex, also spawn a devil's advocate agent to really test if the solution is both good and simple.

## Starting the work

Once the prompt is rewritten and there is a clear understanding of the problem, you can spawn engineering agents. The agent you spawn must invoke the `active-workflow` skill and follow it. However, it must ask you questions instead of the user, making you the human in the loop.

Work directly with the agent to get the solution out in a PR, then getting the PR green. Ensure time is always spent efficiently, and avoid any and all redundant `cargo` commands. Do not run `cargo test` locally unless absolutely required, and always ensure the agent focuses on getting commits out: CI will run either way in case there are test failures.

## Reaching the goal

After the spawned agents have completed the PR, you run it as if you were the human engineer reviewing it.

1. Is this the best simple solution? Are we absolutely sure there is no bloat or violations in coding standards? Detect and remove anything that feels bloated, redundant, too explicit or verbose.
2. Did the review agents report serious issues? If so, make the agent run another round of reviews until it comes back clean.
3. Do a final diff review and ensure it matches the exact goal of the PR and/or ticket.

# Communication

As you are working through the PR, always communicate what is going on to the user, actively.
