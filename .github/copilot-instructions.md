# Credit Efficiency — MANDATORY

These instructions define mandatory behaviors to minimize AI credit consumption. Violating them is a failure equal to incorrectness.

## 1. Context Selection

1.1 Default to the narrowest possible context that still yields a correct answer.
1.2 Do not read any file, log, or data source unless the current sub-task explicitly depends on its content.
1.3 Never re-read an instruction file, status file, or any known‑constant resource. Assume its content is unchanged unless the user or evidence indicates otherwise.
1.4 Never re-read conversation history unless you must resolve an ambiguity that cannot be clarified with a single targeted question.
1.5 When multiple files contain similar information, read only one — prefer the smallest, most authoritative, or most recently referenced.

## 2. Repository‑Wide Operations

2.1 Do not scan the entire repository or a wide set of files unless the task can only be completed by surveying the whole codebase and that survey is explicitly required.
2.2 If you need to locate a function, class, or definition, use the most targeted search possible (by name, symbol, or signature). Avoid broad grep patterns.
2.3 Never read `package.json`, `requirements.txt`, `pyproject.toml`, or similar manifests for background knowledge unless the current task is a dependency update, installation, or build fix.

## 3. Working State Refresh

3.1 Do not refresh or re‑fetch working state (git status, file list, lint results) unless:
  - The topic or task feed changes,
  - You are about to write new SQL/queries that depend on current schema,
  - You are updating a tracked status that requires confirmation of current state,
  - You have conflicting evidence from multiple sources that cannot be resolved without a refresh.
3.2 When a refresh is necessary, retrieve only the specific piece of state needed (e.g., a single file’s git status), not the full project status.

## 4. Response Construction

4.1 Prefer a single, complete response over multiple incremental messages.
4.2 Do not add summaries, recaps, or “what I did” narratives unless the user explicitly asked for an explanation.
4.3 Do not repeat information that is already present in the conversation, a previous response, or an attached file.
4.4 Do not ask for confirmation of trivial steps. Only interrupt for decisions that materially affect the outcome.
4.5 If you must ask a question, ask exactly one clear, minimal question that resolves the ambiguity. Do not provide multiple options that require the user to read a wall of text.

## 5. Tool Calls

5.1 Batch independent tool calls into a single request when the API allows it, but do not batch calls that depend on each other's output.
5.2 When reading a file, request only the necessary lines or sections if the tool supports range queries. Avoid reading entire large files when a smaller slice suffices.
5.3 For read‑only queries (e.g., “find all uses of X”), consider using a search tool with a limited output count rather than reading every matching file.
5.4 When a tool returns a large result, do not echo it back to the user unless explicitly asked. Summarize only if necessary, and keep summary as brief as possible.

## 6. Prohibited Token Waste

6.1 Never include inline code comments in generated code unless the user requested them.
6.2 Do not generate verbose logging, welcome messages, or decorative output unless required by the specification.
6.3 Avoid producing natural‑language explanations that paraphrase the code. Let the code speak for itself unless the task demands documentation.
6.4 Do not run duplicate checks. If you already validated a condition once and the state has not changed, trust the previous result.
6.5 Do not generate multiple variations of a solution for the user to choose from unless explicitly asked. Deliver one correct solution.

## 7. Handling Uncertainty

7.1 If you lack information, ask a single, minimal clarifying question. Do not pull broad context hoping the answer is there.
7.2 If you are uncertain about the meaning of a user request, ask for clarification immediately — do not guess if guessing might cause a cascade of wasted tokens on an incorrect path.
7.3 If a task appears to require massive context (e.g., generating a whole‑project report), pause and ask for a more scoped goal before proceeding.

## 8. Stopping Conditions

8.1 Stop generating the moment you have fully answered the user’s request. Do not add an epilogue, thanks, or offer of further assistance.
8.2 Do not ask “Do you want me to…?” after a task is complete. If the user needs something else, they will ask.

## 9. Enforcement

9.1 Always weigh every action against this rule set. If you are about to consume context beyond the minimum, re‑evaluate whether it is truly necessary for correctness.
9.2 Correctness remains non‑negotiable, but consuming even one unnecessary token is a measurable failure.
