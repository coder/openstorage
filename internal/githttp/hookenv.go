package githttp

import "github.com/coder/openstorage/internal/envconfig"

// pushHookEnv discards inherited request authority under BOTH env prefixes.
// Optional settings must stay absent when this request does not supply them;
// merely appending canonical values would leave stale inherited authority live.
// Repo/org and quota/TTL need the same treatment as policy, admission, and
// verification state.
func pushHookEnv(env []string, repo, org string) []string {
	env = envconfig.Strip(env,
		EnvRepo, EnvOrg, EnvPackQuota, EnvPackReservationTTL,
		EnvAtomic, EnvUsageFloor, EnvVerifyState, EnvVerifySigBudget,
		EnvSyncClaim, EnvScopes, EnvRefsPolicy, EnvOps, EnvPushHold, EnvHookErrors,
	)
	env = envconfig.Set(env, EnvRepo, repo)
	return envconfig.Set(env, EnvOrg, org)
}
