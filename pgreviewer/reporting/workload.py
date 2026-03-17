def format_workload_stats(context: dict) -> str | None:
    """Format context['workload_stats'] as a human-readable production workload line."""
    stats = context.get("workload_stats")
    if not isinstance(stats, dict):
        return None
    calls = stats.get("calls_per_day")
    avg_time = stats.get("avg_time_ms")
    total_minutes = stats.get("total_time_min_per_day")
    if not isinstance(calls, int | float):
        return None
    if not isinstance(avg_time, int | float):
        return None
    if not isinstance(total_minutes, int | float):
        return None
    avg_time_value = float(avg_time)
    avg_time_display = (
        f"{avg_time_value:.0f}"
        if avg_time_value.is_integer()
        else f"{avg_time_value:.1f}"
    )
    return (
        f"Calls: {int(calls):,}/day | Avg time: {avg_time_display}ms | "
        f"Total time: {total_minutes:.1f} min/day"
    )
