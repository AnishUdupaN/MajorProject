"""Core shared package for distributed compute system."""

from __future__ import annotations

# 1. Constants
from core.constants import (
    CONFIG_KEY_EXECUTE_COMMAND,
    CONFIG_KEY_MAX_NODES,
    CONFIG_KEY_MERGE_COMMAND,
    CONFIG_KEY_SPLIT_COMMAND,
    CONFIG_KEYS,
    CONTROL_PORT,
    DAEMON_LISTENING_PREFIX,
    DAEMON_READY_TIMEOUT_SECONDS,
    DEFAULT_INPUT_VIDEO,
    DEVICE_ID_PREFIX,
    DEVICES_JSON_FILE,
    DISCOVERY_BEACON_TYPE,
    FIXED_PORT,
    MASTER_DIRECTORY,
    MASTER_INPUT_DIRECTORY,
    MASTER_OFFER_TYPE,
    MASTER_OUTPUT_DIRECTORY,
    MDNS_ADDR,
    MDNS_KEYWORD,
    MDNS_KEYWORD_MASTER,
    MDNS_KEYWORD_WORKER,
    MDNS_MASTER_FQDN,
    MDNS_PORT,
    MDNS_SERVICE_MASTER,
    MDNS_SERVICE_WORKER,
    MDNS_WORKER_FQDN,
    MESSAGE_REQUIRED_KEYS,
    MESSAGE_TYPE_AUTH,
    MESSAGE_TYPE_BINARY_INFO,
    MESSAGE_TYPE_BINARY_INFO_REQUEST,
    MESSAGE_TYPE_BINARY_READY,
    MESSAGE_TYPE_FAILED,
    MESSAGE_TYPE_FILE_RECEIVED,
    MESSAGE_TYPE_FINISHED,
    MESSAGE_TYPE_PIN_ACCEPTED,
    MESSAGE_TYPE_PIN_CHALLENGE,
    MESSAGE_TYPE_PIN_REJECTED,
    MESSAGE_TYPE_PIN_RESPONSE,
    MESSAGE_TYPE_READY,
    MESSAGE_TYPE_SHUTDOWN,
    MESSAGE_TYPE_WORKER_TELEMETRY,
    PART_FILENAME_PREFIX,
    PART_FILENAME_SUFFIX,
    PROGRESS_EXECUTING,
    PROGRESS_FINISHED,
    PROGRESS_MERGING_FILES,
    PROGRESS_RECEIVING_FILES,
    PROGRESS_SENDING_FILE,
    PROGRESS_SPLITTING_FILE,
    SHARED_SECRET_ENV_VAR,
    UDP_DISCOVERY_PORT,
    WORKER_BINARY_DIRECTORY,
    WORKER_DIRECTORY,
    WORKER_INPUT_DIRECTORY,
    WORKER_OUTPUT_DIRECTORY,
)

# 2. Config
from core.config import (
    Config,
    get_available_configs,
    load_config,
)

# 3. Devices
from core.devices import (
    assign_device_id,
    build_active_device_mapping,
    swap_device_ip,
    write_devices_json,
)

# 4. Binaries
from core.binaries import (
    BINARY_PLATFORM_MAP,
    compute_md5,
    get_binary_base_dir,
    get_binary_path,
    get_worker_platform,
    resolve_worker_binary_base_dir,
    resolve_worker_binary_cache_path,
)

# 5. Messaging & Protocol
from core.messaging import (
    MasterShutdownError,
    _SOCKET_BUFFERS,
    generate_pin,
    get_shared_secret,
    has_buffered_message,
    receive_binary_info,
    receive_binary_info_request,
    receive_binary_ready,
    receive_failed_message,
    receive_file_received_message,
    receive_finished_message,
    receive_json_message,
    receive_pin_challenge,
    receive_pin_response,
    receive_ready_message,
    receive_worker_telemetry,
    send_auth_message,
    send_binary_info,
    send_binary_info_request,
    send_binary_ready,
    send_failed_message,
    send_file_received_message,
    send_finished_message,
    send_json_message,
    send_pin_accepted,
    send_pin_challenge,
    send_pin_rejected,
    send_pin_response,
    send_ready_message,
    send_shutdown_message,
    send_worker_telemetry,
    validate_message,
    verify_auth_message,
)

# 6. Discovery & Networking
from core.discovery import (
    MdnsAnnouncer,
    build_a_record,
    build_mdns_announcement,
    build_ptr_record,
    discover_master_ip,
    discover_master_udp,
    encode_dns_label,
    extract_a_record_ip,
    get_local_ip,
    get_local_ip_for_peer,
    make_mdns_listener_socket,
    make_mdns_sender_socket,
    parse_dns_name,
    start_udp_discovery_listener,
)

# 7. File Transfer
from core.file_transfer import (
    FileTransferDaemon,
    _active_daemons_registry,
    _cleanup_all_daemons,
    _make_handler_class,
    _parent_watchdog,
    parse_daemon_arguments,
    read_daemon_port,
    request_file,
    request_file_list,
    run_file_transfer_daemon,
    start_file_transfer_daemon,
    upload_result_file,
    wait_for_daemon_port,
)

# 8. Benchmark & Telemetry
from core.benchmark import (
    _get_total_ram_mb,
    benchmark_multi_core,
    benchmark_single_core,
    detect_cpu_name,
    detect_gpu_info,
    detect_power_state,
    get_num_cpus,
    get_process_cpu_percent,
    get_system_cpu_percent,
    rank_workers_by_affinity,
    run_node_benchmark,
    sensors_battery,
)

# 9. Execution & Process Tracking
from core.execution import (
    ProcessCpuTracker,
    _stderr_reader_thread,
    build_execute_command,
    check_socket_connection_lost,
    force_stop_process,
    format_elapsed_time,
    get_video_duration_ffprobe,
    parse_ffmpeg_time_seconds,
    run_execute_command,
    set_process_nice,
)

# 10. Task Preparation & Merging
from core.task_prep import (
    build_merge_command,
    build_split_command,
    cleanup_master_temporary_files,
    cleanup_worker_temporary_files,
    generate_filelist_text,
    get_part_extension,
    get_video_duration_seconds,
    part_filename_for_node,
    resolve_input_video,
    resolve_output_file,
    run_merge_command,
    run_split_command,
    verify_part_files,
    write_filelist_txt,
)

# 11. Dashboard & UI
from core.dashboard import (
    _LOG_LOCK,
    StatusDashboard,
    printlog,
)
