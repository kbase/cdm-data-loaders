"""Expected PyArrow schema for real-world example cts_jobs."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "jobs",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field(
                                "error",
                                pa.string(),
                                nullable=True,
                                metadata={
                                    "comment": "A description of the error that occurred.",
                                    "jsonschema_type": "string",
                                },
                            ),
                            pa.field(
                                "output_file_count",
                                pa.int64(),
                                nullable=True,
                                metadata={
                                    "comment": "The number of output files, if available.",
                                    "jsonschema_type": "integer",
                                },
                            ),
                            pa.field(
                                "id",
                                pa.string(),
                                nullable=False,
                                metadata={
                                    "comment": "An opaque, unique string that serves as the job's ID.",
                                    "jsonschema_type": "string",
                                },
                            ),
                            pa.field(
                                "state",
                                pa.string(),
                                nullable=False,
                                metadata={"comment": "The state of a job.", "jsonschema_type": "string"},
                            ),
                            pa.field(
                                "transition_times",
                                pa.list_(
                                    pa.field(
                                        "item",
                                        pa.struct(
                                            [
                                                pa.field(
                                                    "state",
                                                    pa.string(),
                                                    nullable=False,
                                                    metadata={
                                                        "comment": "The state of a job.",
                                                        "jsonschema_type": "string",
                                                    },
                                                ),
                                                pa.field(
                                                    "time",
                                                    pa.string(),
                                                    nullable=False,
                                                    metadata={
                                                        "comment": "The time at which the new state was entered.",
                                                        "jsonschema_type": "string (date-time)",
                                                    },
                                                ),
                                            ]
                                        ),
                                        nullable=False,
                                        metadata={
                                            "comment": "Denotes the new state and the entry time when a state change occurs."
                                        },
                                    )
                                ),
                                nullable=False,
                                metadata={"comment": "A list of job state transitions."},
                            ),
                            pa.field(
                                "user",
                                pa.string(),
                                nullable=False,
                                metadata={"comment": "The user that ran the job.", "jsonschema_type": "string"},
                            ),
                            pa.field(
                                "admin_meta",
                                pa.map_(
                                    pa.string(),
                                    pa.field(
                                        "value",
                                        pa.string(),
                                        nullable=False,
                                        metadata={"jsonschema_type": "string | integer | number"},
                                    ),
                                ),
                                nullable=True,
                                metadata={
                                    "comment": "Metadata for the job set by service administrators. This metadata is mutable and arbitrary from the point of view of the service."
                                },
                            ),
                            pa.field(
                                "job_input",
                                pa.struct(
                                    [
                                        pa.field(
                                            "cluster",
                                            pa.string(),
                                            nullable=False,
                                            metadata={
                                                "comment": "All clusters that have ever been registered in the system, including those that are no longer\nregistered but may have associated job records.\n\nperlmutter-jaws: The Perlmutter cluster at NERSC run via JAWS.  \nlawrencium-jaws: The Lawrencium cluster at LBNL run via JAWS.  \nkbase: KBase compute nodes.",
                                                "jsonschema_type": "string",
                                            },
                                        ),
                                        pa.field(
                                            "image",
                                            pa.string(),
                                            nullable=False,
                                            metadata={
                                                "comment": "The Docker image to run for the job. Include the SHA to ensure the exact code requested is run.",
                                                "jsonschema_type": "string",
                                            },
                                        ),
                                        pa.field(
                                            "params",
                                            pa.struct(
                                                [
                                                    pa.field(
                                                        "input_mount_point",
                                                        pa.string(),
                                                        nullable=True,
                                                        metadata={
                                                            "comment": "Where to place input files in the container. Must start from the container root and include at least one directory when resolved.",
                                                            "jsonschema_type": "string",
                                                        },
                                                    ),
                                                    pa.field(
                                                        "output_mount_point",
                                                        pa.string(),
                                                        nullable=True,
                                                        metadata={
                                                            "comment": "Where output files should be written in the container. Must start from the container root and include at least one directory when resolved. Note that the mount point is not included in the file paths thatare eventually transferred to S3.",
                                                            "jsonschema_type": "string",
                                                        },
                                                    ),
                                                    pa.field(
                                                        "declobber",
                                                        pa.bool_(),
                                                        nullable=True,
                                                        metadata={
                                                            "comment": "If true, prepend the container number to all output file paths as a directory. This prevents containers that write to the same file path from clobbering each other and losing data. If the output files of containers are guaranteed to be unique (e.g. output file names are based on input file names and input file names are unique) then this setting is not necessary. The same effect can be created by use of a container number parameter when specifying the output directory in the arguments or environment.",
                                                            "jsonschema_type": "boolean",
                                                        },
                                                    ),
                                                    pa.field(
                                                        "refdata_mount_point",
                                                        pa.string(),
                                                        nullable=True,
                                                        metadata={
                                                            "comment": "Where reference data files should be pleased in the container. Must start from the container root and include at least one directory when resolved. Required if the job image requires reference data and a default reference data mount point isn't specified for the image. If a default is provided, this setting overrides it.",
                                                            "jsonschema_type": "string",
                                                        },
                                                    ),
                                                    pa.field(
                                                        "args",
                                                        pa.list_(
                                                            pa.field(
                                                                "item",
                                                                pa.struct(
                                                                    [
                                                                        pa.field(
                                                                            "type",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "The type of a parameter if not a string literal.\n\ninput_files will cause the list of input files for the container to be inserted\ninto the entrypoint command line or an environment variable.\n\nmanifest_file will cause the list of input files for the container to be placed in a\nmanifest file and will insert the file location into the entrypoint command line\nor an environment variable.\n\ncontainer_number will insert the integer number of the container into the\nentrypoint command line or an environment variable. This can be used so prevent file path\ncollisions if multiple containers write to the same file paths.",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "input_files_format",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "The format of an input files type parameter.\n\nComma and space separated lists format the list of files as one would expect.\n\nRepeat parameter causes the input files to be inserted as a space separated list,\nwith each file preceded by the parameter flag. Only valid for CLI flag based arguments.\nIf the flag ends with an equals sign, there is no space left between the flag and the\nparameter.\n\nExamples:  \n`-i`: `-i file1 -i file2 ... -i fileN`  \n`--input_file=`: `--input_file=file1 --input_file=file2 ... --input_file=fileN`  ",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "manifest_file_format",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "The format of a manifest file.\n\nfiles will cause the manifest file to consist of a list of the input files, one per line.\n\ndata_ids will cause the manifest file to consist of a list of the data IDs associated with\nthe input files, one per line.",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "manifest_file_header",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "The header for the manifest file, if any.",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "container_num_prefix",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "A string that will be prepended to the container number.",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "container_num_suffix",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "A string that will be appended to the container number.",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "flag",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "A command line argument flag to precede the parameter. If the flag ends with '=', no space is left between the flag and its value.",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "_value",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={"jsonschema_type": "string"},
                                                                        ),
                                                                    ]
                                                                ),
                                                                nullable=False,
                                                            )
                                                        ),
                                                        nullable=True,
                                                        metadata={
                                                            "comment": "A list of command line arguments to be inserted after the container entrypoint command. Strings are treated as literals."
                                                        },
                                                    ),
                                                    pa.field(
                                                        "environment",
                                                        pa.map_(
                                                            pa.string(),
                                                            pa.field(
                                                                "value",
                                                                pa.struct(
                                                                    [
                                                                        pa.field(
                                                                            "type",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "The type of a parameter if not a string literal.\n\ninput_files will cause the list of input files for the container to be inserted\ninto the entrypoint command line or an environment variable.\n\nmanifest_file will cause the list of input files for the container to be placed in a\nmanifest file and will insert the file location into the entrypoint command line\nor an environment variable.\n\ncontainer_number will insert the integer number of the container into the\nentrypoint command line or an environment variable. This can be used so prevent file path\ncollisions if multiple containers write to the same file paths.",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "input_files_format",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "The format of an input files type parameter.\n\nComma and space separated lists format the list of files as one would expect.\n\nRepeat parameter causes the input files to be inserted as a space separated list,\nwith each file preceded by the parameter flag. Only valid for CLI flag based arguments.\nIf the flag ends with an equals sign, there is no space left between the flag and the\nparameter.\n\nExamples:  \n`-i`: `-i file1 -i file2 ... -i fileN`  \n`--input_file=`: `--input_file=file1 --input_file=file2 ... --input_file=fileN`  ",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "manifest_file_format",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "The format of a manifest file.\n\nfiles will cause the manifest file to consist of a list of the input files, one per line.\n\ndata_ids will cause the manifest file to consist of a list of the data IDs associated with\nthe input files, one per line.",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "manifest_file_header",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "The header for the manifest file, if any.",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "container_num_prefix",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "A string that will be prepended to the container number.",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "container_num_suffix",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={
                                                                                "comment": "A string that will be appended to the container number.",
                                                                                "jsonschema_type": "string",
                                                                            },
                                                                        ),
                                                                        pa.field(
                                                                            "_value",
                                                                            pa.string(),
                                                                            nullable=True,
                                                                            metadata={"jsonschema_type": "string"},
                                                                        ),
                                                                    ]
                                                                ),
                                                                nullable=False,
                                                            ),
                                                        ),
                                                        nullable=True,
                                                        metadata={
                                                            "comment": "A dictionary of environment variables to be inserted into the container. String values are treated as literals."
                                                        },
                                                    ),
                                                ]
                                            ),
                                            nullable=False,
                                            metadata={
                                                "comment": "A set of parameters for a container in a job.\n\nSpecifying files as part of arguments to a job can be done in three ways:\n\n* If the entrypoint command takes a directory as an argument, that\n  directory can be specified literally in an argument. All files will be available in the\n  input mount point, with paths resulting from the path in S3 as well as the input_roots\n  parameter from the job input.\n* If individual filenames need to be specified, use a Parameter instance in\n  the positional or environmental arguments. At most one Parameter can\n  be specified per parameter set.\n* A Parameter instance can also be used to specify a manifest of file names or\n  file IDs to be passed to the container."
                                            },
                                        ),
                                        pa.field(
                                            "num_containers",
                                            pa.int64(),
                                            nullable=True,
                                            metadata={
                                                "comment": "The number of containers to run in parallel. Input files will be split between the containers.",
                                                "jsonschema_type": "integer",
                                            },
                                        ),
                                        pa.field(
                                            "cpus",
                                            pa.int64(),
                                            nullable=True,
                                            metadata={
                                                "comment": "The number of CPUs to allocate per container.",
                                                "jsonschema_type": "integer",
                                            },
                                        ),
                                        pa.field(
                                            "gpus",
                                            pa.int64(),
                                            nullable=True,
                                            metadata={
                                                "comment": "The number of GPUs to allocate per container. Jobs requesting 0 GPUs are never scheduled on GPU-capable nodes.",
                                                "jsonschema_type": "integer",
                                            },
                                        ),
                                        pa.field(
                                            "memory",
                                            pa.int64(),
                                            nullable=True,
                                            metadata={
                                                "comment": "The amount of memory to allocate per container either as the number of bytes or a specification string such as 100MB, 2GB, etc.",
                                                "jsonschema_type": "integer",
                                            },
                                        ),
                                        pa.field(
                                            "runtime",
                                            pa.string(),
                                            nullable=True,
                                            metadata={
                                                "comment": "The runtime required for each container as the number of seconds or an ISO8601 duration string.",
                                                "jsonschema_type": "string (duration)",
                                            },
                                        ),
                                        pa.field(
                                            "input_roots",
                                            pa.list_(
                                                pa.field(
                                                    "item",
                                                    pa.string(),
                                                    nullable=False,
                                                    metadata={"jsonschema_type": "string"},
                                                )
                                            ),
                                            nullable=True,
                                            metadata={
                                                "comment": 'If specified, preserves file hierarchies for S3 files below the given root paths, starting from the bucket. Any files that are not prefixed by a root path are placed in the root directory of the job input dir. If any input files have the same path in the input dir, the job fails. For example, given a input_roots entry of ["mybucket/foo/bar"] and the input files ["otherbucket/foo", "mybucket/bar", "mybucket/foo/bar/baz/bat"] the job input directory would include the files foo, bar, and baz/bat. To preserve hierarchies for all files, set input_roots to [""].  If an input file matches more than one root, the longest root that isn\'t identical to the file path is used. Whitespace on the left side of the path is is ignored. Duplicate roots are ignored.'
                                            },
                                        ),
                                        pa.field(
                                            "output_dir",
                                            pa.string(),
                                            nullable=False,
                                            metadata={
                                                "comment": "The S3 folder, starting with the bucket, in which to place results.",
                                                "jsonschema_type": "string",
                                            },
                                        ),
                                    ]
                                ),
                                nullable=False,
                                metadata={
                                    "comment": "Input to a Job, consisting of fields containing small amounts of data.\nSuitable for a list of jobs."
                                },
                            ),
                            pa.field(
                                "image",
                                pa.struct(
                                    [
                                        pa.field(
                                            "registered_by",
                                            pa.string(),
                                            nullable=False,
                                            metadata={
                                                "comment": "The username of the user that performed the registration.",
                                                "jsonschema_type": "string",
                                            },
                                        ),
                                        pa.field(
                                            "registered_on",
                                            pa.string(),
                                            nullable=False,
                                            metadata={
                                                "comment": "The time of registration.",
                                                "jsonschema_type": "string (date-time)",
                                            },
                                        ),
                                        pa.field(
                                            "name",
                                            pa.string(),
                                            nullable=False,
                                            metadata={
                                                "comment": "The normalized name of the a docker image, consisting of the host and path.",
                                                "jsonschema_type": "string",
                                            },
                                        ),
                                        pa.field(
                                            "digest",
                                            pa.string(),
                                            nullable=False,
                                            metadata={"comment": "The image digest.", "jsonschema_type": "string"},
                                        ),
                                        pa.field(
                                            "entrypoint",
                                            pa.list_(
                                                pa.field(
                                                    "item",
                                                    pa.string(),
                                                    nullable=False,
                                                    metadata={"jsonschema_type": "string"},
                                                )
                                            ),
                                            nullable=False,
                                            metadata={"comment": "The entrypoint command extracted from the image."},
                                        ),
                                        pa.field(
                                            "tag",
                                            pa.string(),
                                            nullable=True,
                                            metadata={
                                                "comment": "The image tag at registration time. The tag may no longer point to the same image.",
                                                "jsonschema_type": "string",
                                            },
                                        ),
                                        pa.field(
                                            "refdata_id",
                                            pa.string(),
                                            nullable=True,
                                            metadata={
                                                "comment": "The ID of reference data associated with the image. The image requires this reference data to run, and it will be mounted into the image container at the refdata mount point.",
                                                "jsonschema_type": "string",
                                            },
                                        ),
                                        pa.field(
                                            "default_refdata_mount_point",
                                            pa.string(),
                                            nullable=True,
                                            metadata={
                                                "comment": "The default mount point for the image refdata in the container. Overridden by the refdata mount point in a job submission if provided there. If a default mount point is provided, then providing a mount point in the job submission is optional. Must be an absolute path.",
                                                "jsonschema_type": "string",
                                            },
                                        ),
                                    ]
                                ),
                                nullable=False,
                                metadata={
                                    "comment": "Information about a Docker image as stored in a Job data structure."
                                },
                            ),
                            pa.field(
                                "input_file_count",
                                pa.int64(),
                                nullable=False,
                                metadata={"comment": "The number of input files.", "jsonschema_type": "integer"},
                            ),
                            pa.field(
                                "cpu_hours",
                                pa.float64(),
                                nullable=True,
                                metadata={
                                    "comment": "The total CPU hours used by the job, if available.",
                                    "jsonschema_type": "number",
                                },
                            ),
                            pa.field(
                                "cpu_factor",
                                pa.float64(),
                                nullable=True,
                                metadata={
                                    "comment": "The ratio of the cpu time used to the number of cpus requested times the job's actual time, if available. Perfect usage is 1, < 1 means requested CPUs are partially idle, possibly due to memory or IO bounded work, > 1 means more CPU usage than requested.",
                                    "jsonschema_type": "number",
                                },
                            ),
                            pa.field(
                                "max_memory",
                                pa.int64(),
                                nullable=True,
                                metadata={
                                    "comment": "The maximum memory in bytes used by a single container in the job, if available.",
                                    "jsonschema_type": "integer",
                                },
                            ),
                            pa.field(
                                "logpath",
                                pa.string(),
                                nullable=True,
                                metadata={
                                    "comment": "A location in S3 where the logfiles for the job containers can be viewed.",
                                    "jsonschema_type": "string",
                                },
                            ),
                        ]
                    ),
                    nullable=False,
                    metadata={
                        "comment": "Information about a job, consisting of fields containing small amounts of data.\nSuitable for a list of jobs."
                    },
                )
            ),
            nullable=False,
            metadata={"comment": "The jobs"},
        )
    ]
)
