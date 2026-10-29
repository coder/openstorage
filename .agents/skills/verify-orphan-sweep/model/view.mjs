// View for the pstack /statechart skill: labels PackSweep states and steps
// in the words of the code. It follows writer w1 and sweeper s1, the two
// processes in every shortest bug path the configs produce.

const write = {
  idle: "not started",
  upload: "uploading K",
  register: "before RegisterPacks",
  verify: "before EnsurePublished",
  done: "write succeeded",
  failed: "write failed",
  crashed: "write died",
};

const sweep = {
  idle: "between sweeps",
  heal_next: "heal: next backup",
  heal_reg: "heal: reads registry",
  heal_stat: "heal: K registered, stats K",
  heal_purge: "heal: at the purge age gate",
  rv_reg: "revalidate: reads registry",
  sd_copy: "copies K to quarantine",
  sd_delete: "deletes K",
  sd_recheck: "rechecks registry",
  sd_restore: "restores K from quarantine",
};

const backups = (v) => {
  const seen = Object.values(v.backup).filter((b) => b !== "unused");
  if (seen.length === 0) return "no backup";
  const words = { fresh: "backup", aged: "backup past purge age", gone: "backup purged" };
  return seen.map((b) => words[b]).join(", ");
};

export const state = (v) => ({
  Pack: `${v.registered ? "registered" : "unregistered"}, ${v.present ? "bytes present" : "bytes missing"}`,
  Quarantine: backups(v),
  Write: write[v.wpc.w1],
  Sweeper: sweep[v.spc.s1],
});

const steps = {
  WriterStat: "the write stats K",
  WriterUpload: "the write uploads K",
  WriterRegister: "the write runs RegisterPacks",
  WriterVerify: "EnsurePublished re-uploads K if missing",
  WriterCrash: "the write dies or errors",
  SweepStart: "a sweep starts",
  HealReg: "the heal reads the registry",
  HealStat: "the heal restores a missing registered K",
  HealPurge: "the heal purges an aged backup",
  RevalidateReg: "revalidateOrphan reads the registry",
  QuarantineCopy: "safeDelete copies K to quarantine",
  Delete: "safeDelete deletes K",
  Recheck: "safeDelete rechecks the registry",
  Restore: "safeDelete restores K",
  SweeperCrash: "the sweeper dies",
  Fold: "a repack folds K away",
  AgeBackup: "the backup passes quarantinePurgeAge",
};

export const step = (action, from, to) => {
  if (action === "HealNext") return to.spc.s1 === "heal_reg" ? "the heal picks a backup" : "the heal is done";
  return steps[action] ?? action;
};

export const title = "Orphan-pack sweeper vs a pack write (writer w1, sweeper s1)";
