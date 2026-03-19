#!/usr/bin/env Rscript
# dsJobs Worker Daemon
# Launched by .dsjobs_worker_start() as a background R process.
# Usage: Rscript main.R /var/lib/dsjobs

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript main.R <DSJOBS_HOME>")
}

dsjobs_home <- args[1]
options(dsjobs.home = dsjobs_home)

library(dsJobs)
dsJobs:::.worker_main()
