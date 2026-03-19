#!/bin/bash
# Add this to your Rock container's entrypoint or docker-compose.
# Example docker-compose addition:
#
#   dsjobs-worker:
#     image: datashield/rock-base:latest
#     command: ["/bin/bash", "/opt/dsjobs/worker-entrypoint.sh"]
#     volumes:
#       - dsjobs_data:/var/lib/dsjobs
#     restart: unless-stopped
#
# Or add to existing Rock entrypoint:
#   Rscript /usr/local/lib/R/site-library/dsJobs/worker/main.R /var/lib/dsjobs &

DSJOBS_HOME="${DSJOBS_HOME:-/var/lib/dsjobs}"
mkdir -p "$DSJOBS_HOME"/{runners,artifacts,publish,locks}

exec Rscript /usr/local/lib/R/site-library/dsJobs/worker/main.R "$DSJOBS_HOME"
