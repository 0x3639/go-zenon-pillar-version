# Build and dependency policy

All dependency installation and execution must happen inside containers.
Reading and editing source on the host is allowed.

- Use `./scripts/check.sh` for Go dependency resolution, tests, static checks and builds.
- Do not run dependency-loading Go or Node programs on the host.
- Optional Node tooling must use `nodebox npm install`, `nodebox npm ci`,
  `nodebox npm run <script>`, `nodebox npx <tool>`, or `nodebox node <file>`.
- If `nodebox` is unavailable, stop rather than running Node dependencies on the host.
- Use Docker Compose for long-running services. Publish local preview ports only
  on localhost and bind the service to `0.0.0.0` inside the container.
- Only paths under `/Users/user` can be mounted by this workstation's Colima setup.

# Project boundaries

This is the independent Zenon observer repository. Follow the staged acceptance
criteria in `SPEC.md`. The existing `go-zenon-version` application and discovery
API are read-only references unless the user explicitly requests integration.
The user performs Coolify deployment.

Keep relay evidence separate from producer identity. Advertised relay versions
must not silently become producer software versions or automatic identity
mappings. Preserve unknown values and report incomplete milestones honestly.
