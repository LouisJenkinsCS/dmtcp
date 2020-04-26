#include <stdio.h>
#include "dmtcp.h"
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>

static const char *outputFile;
static int outputFileFD;

static double startTime;
static pid_t pid = -1;

enum profiler_event_type {
	PROFILER_EVENT_TYPE_NONE,
	PROFILER_EVENT_TYPE_RESTART,
	PROFILER_EVENT_TYPE_CHECKPOINT
};

static int exitOnCheckpointRestart = 0;

static double eventTime;
static enum profiler_event_type eventType;
static int is_initialized = 0;

static double current_time() {
	struct timespec spec;
	clock_gettime(CLOCK_MONOTONIC, &spec);
	double sec = spec.tv_sec + (double)(spec.tv_nsec / (double)1e9);
	return sec;
}

static double diff_time(double other) {
	double now = current_time();
	return now - other;
}

static uint64_t epochTime() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (uint64_t)(tv.tv_sec) * 1000 + (uint64_t)(tv.tv_usec) / 1000;
}

void event_hook(DmtcpEvent_t event, DmtcpEventData_t *data)
{
	if (pid == -1) {
		printf("Pid=%d,PPid=%d\n", getpid(), getppid());
		pid = getpid();
	} else if (pid != getpid()) {
		return; // Do not process forks
	}
	switch(event) {
		case DMTCP_EVENT_INIT:{
			if (is_initialized) {
				break;
			} else {
				is_initialized = 1;
			}
			startTime = current_time();
			outputFile = getenv("DMTCP_PROFILER_FILE");
			if (outputFile == NULL || strlen(outputFile) == 0) {
				outputFile = "/tmp/DMTCP_profile.out";
			}
			outputFileFD = open(outputFile, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
			if (outputFileFD == -1) {
				printf("[DMTCP_profiler] Unable to open file %s!\n", outputFile);
			} else {
				printf("[DMTCP_profiler] Logging to file %s\n", outputFile);
			}
			event = PROFILER_EVENT_TYPE_NONE;
			
			char *tmp = getenv("DMTCP_PROFILER_KILL");
			if (tmp != NULL) {
				exitOnCheckpointRestart=1;
			}
			break;
		}
		case DMTCP_EVENT_EXIT: {
			char buf[1024];
			snprintf(buf, 1024, "Total time is %fs\n", diff_time(startTime));
			write(outputFileFD, buf, strlen(buf));
			close(outputFileFD);
			printf("%s\n", buf);
			break;
		}
		case DMTCP_EVENT_PRECHECKPOINT: {
			eventType = PROFILER_EVENT_TYPE_CHECKPOINT;
			eventTime = current_time();
			char *msg = "DMTCP_EVENT_PRECHECKPOINT\n";
			write(outputFileFD, msg, strlen(msg));
			break;
		}
		case DMTCP_EVENT_RESTART: {
			eventType = PROFILER_EVENT_TYPE_NONE;
			char *msg = "DMTCP_EVENT_RESTART\n";
			write(outputFileFD, msg, strlen(msg));
			char buf[1024];
			snprintf(buf, 1024, "Restart Epoch Time: %lu\n", epochTime());
			write(outputFileFD, buf, strlen(buf));
			if (exitOnCheckpointRestart) {
				exit(EXIT_SUCCESS);
			}
			break;
		}
		case DMTCP_EVENT_RESUME: {
			char *msg = "DMTCP_EVENT_RESUME\n";
			write(outputFileFD, msg, strlen(msg));
			if (eventType == PROFILER_EVENT_TYPE_CHECKPOINT) {
				char buf[1024];
				snprintf(buf, 1024, "Checkpoint took %fs\n", diff_time(eventTime));
				write(outputFileFD, buf, strlen(buf));
				if (exitOnCheckpointRestart) {
					exit(EXIT_SUCCESS);
				}
			} else if (eventType == PROFILER_EVENT_TYPE_RESTART) {
				char buf[1024];
				snprintf(buf, 1024, "Restart took %fs\n", diff_time(eventTime));
				write(outputFileFD, buf, strlen(buf));
			} else {
				char buf[1024];
				snprintf(buf, 1024, "Unknown event %d!\n", eventType);
				write(outputFileFD, buf, strlen(buf));	
			}
			eventType = PROFILER_EVENT_TYPE_NONE;
			break;
		}
	}
}

DmtcpPluginDescriptor_t profiler_plugin = {
		DMTCP_PLUGIN_API_VERSION,
		DMTCP_PACKAGE_VERSION,
		"profiler",
		"DMTCP",
		"dmtcp@ccs.neu.edu",
		"Profiler for DMTCP Checkpoints",
		event_hook
};

DMTCP_DECL_PLUGIN(profiler_plugin);
