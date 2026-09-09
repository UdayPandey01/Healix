const GITHUB_API = "https://api.github.com";

export type FileChange = { path: string; content: string };
export type OpenPrResult =
    | { ok: true; url: string; number: number; branch: string; alreadyExisted: boolean }
    | { ok: false; error: string };

export function githubConfig() {
    return {
        token: process.env.GITHUB_TOKEN,
        repo: process.env.GITHUB_REPO,
        base: process.env.GITHUB_BASE_BRANCH ?? "main",
    };
}

export function isConfigured(): boolean {
    const { token, repo } = githubConfig();
    return Boolean(token && repo);
}

export function branchForRun(runId: string): string {
    return `healix/run-${runId}`;
}

export function prBody(body: string, runId: string): string {
    return [
        body,
        "",
        "---",
        `Opened by Healix for run \`${runId}\` after human approval.`,
        "Healix never merges its own pull requests.",
    ].join("\n");
}

export function normaliseFiles(input: unknown): FileChange[] {
    if (!Array.isArray(input)) return [];

    const files: FileChange[] = [];
    for (const entry of input) {
        if (!entry || typeof entry !== "object") continue;

        const path = String((entry as { path?: unknown }).path ?? "").trim();
        const content = (entry as { content?: unknown }).content;

        if (path === "" || typeof content !== "string") continue;
        if (path.startsWith("/") || path.split("/").includes("..")) continue;

        files.push({ path, content });
    }

    return files;
}

async function findStaleFiles(
    token: string,
    owner: string,
    name: string,
    base: string,
    files: FileChange[],
    localRoot: string,
): Promise<string[]> {
    const { readFile } = await import("node:fs/promises");
    const { join } = await import("node:path");
    const stale: string[] = [];

    for (const file of files) {
        const res = await gh(
            token,
            `/repos/${owner}/${name}/contents/${file.path}?ref=${encodeURIComponent(base)}`,
        );

        if (!res.ok || typeof res.body?.content !== "string") continue;

        const remote = Buffer.from(res.body.content, "base64").toString("utf-8");

        let local: string;
        try {
            local = await readFile(join(localRoot, file.path), "utf-8");
        } catch {
            continue;
        }

        if (remote !== local) stale.push(file.path);
    }

    return stale;
}

async function gh(token: string, path: string, init: RequestInit = {}) {
    const res = await fetch(`${GITHUB_API}${path}`, {
        ...init,
        headers: {
            Authorization: `Bearer ${token}`,
            Accept: "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            ...(init.headers ?? {}),
        },
    });

    const text = await res.text();
    return { ok: res.ok, status: res.status, body: text ? JSON.parse(text) : null };
}

export async function openPullRequest(
    runId: string,
    patch: { title: string; body: string; files: FileChange[] },
    localRoot?: string,
): Promise<OpenPrResult> {
    const { token, repo, base } = githubConfig();
    if (!token || !repo) {
        return { ok: false, error: "GITHUB_TOKEN and GITHUB_REPO must be set." };
    }
    if (patch.files.length === 0) {
        return { ok: false, error: "no files to commit" };
    }

    const [owner, name] = repo.split("/");
    if (!owner || !name) {
        return { ok: false, error: `GITHUB_REPO must be "owner/repo", got "${repo}"` };
    }

    const branch = branchForRun(runId);

    try {
        const existing = await gh(
            token,
            `/repos/${owner}/${name}/pulls?head=${owner}:${branch}&state=open`,
        );
        if (existing.ok && Array.isArray(existing.body) && existing.body.length > 0) {
            const pr = existing.body[0];
            return {
                ok: true,
                url: pr.html_url,
                number: pr.number,
                branch,
                alreadyExisted: true,
            };
        }

        if (localRoot) {
            const stale = await findStaleFiles(token, owner, name, base, patch.files, localRoot);
            if (stale.length > 0) {
                return {
                    ok: false,
                    error:
                        `refusing to open a PR from a stale base: ${stale.join(", ")} ` +
                        `differ between ${base} and the local working copy. The branch moved ` +
                        `after the agent read these files, so committing them would silently ` +
                        `revert that change. Pull the target repo, re-run \`npm run index\`, ` +
                        `and investigate again.`,
                };
            }
        }

        const baseRef = await gh(token, `/repos/${owner}/${name}/git/ref/heads/${base}`);
        if (!baseRef.ok) {
            return { ok: false, error: `could not read base branch "${base}" (HTTP ${baseRef.status})` };
        }
        const baseSha = baseRef.body.object.sha;

        const baseCommit = await gh(token, `/repos/${owner}/${name}/git/commits/${baseSha}`);
        if (!baseCommit.ok) {
            return { ok: false, error: `could not read base commit (HTTP ${baseCommit.status})` };
        }

        const tree = await gh(token, `/repos/${owner}/${name}/git/trees`, {
            method: "POST",
            body: JSON.stringify({
                base_tree: baseCommit.body.tree.sha,
                tree: patch.files.map((f) => ({
                    path: f.path,
                    mode: "100644",
                    type: "blob",
                    content: f.content,
                })),
            }),
        });
        if (!tree.ok) {
            return { ok: false, error: `could not create tree (HTTP ${tree.status})` };
        }

        const commit = await gh(token, `/repos/${owner}/${name}/git/commits`, {
            method: "POST",
            body: JSON.stringify({
                message: `${patch.title}\n\nOpened by Healix for run ${runId}.`,
                tree: tree.body.sha,
                parents: [baseSha],
            }),
        });
        if (!commit.ok) {
            return { ok: false, error: `could not create commit (HTTP ${commit.status})` };
        }

        const ref = await gh(token, `/repos/${owner}/${name}/git/refs`, {
            method: "POST",
            body: JSON.stringify({ ref: `refs/heads/${branch}`, sha: commit.body.sha }),
        });

        if (!ref.ok && ref.status === 422) {
            const updated = await gh(token, `/repos/${owner}/${name}/git/refs/heads/${branch}`, {
                method: "PATCH",
                body: JSON.stringify({ sha: commit.body.sha, force: true }),
            });
            if (!updated.ok) {
                return { ok: false, error: `could not update branch (HTTP ${updated.status})` };
            }
        } else if (!ref.ok) {
            return { ok: false, error: `could not create branch (HTTP ${ref.status})` };
        }

        const pr = await gh(token, `/repos/${owner}/${name}/pulls`, {
            method: "POST",
            body: JSON.stringify({
                title: patch.title,
                body: prBody(patch.body, runId),
                head: branch,
                base,
            }),
        });
        if (!pr.ok) {
            return { ok: false, error: `could not open PR (HTTP ${pr.status})` };
        }

        return {
            ok: true,
            url: pr.body.html_url,
            number: pr.body.number,
            branch,
            alreadyExisted: false,
        };
    } catch (err) {
        return { ok: false, error: err instanceof Error ? err.message : String(err) };
    }
}
