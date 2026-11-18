from pathlib import Path
import pandas as pd


INPUT_FILES = [
    "output/cleaned_trajs/lkpg_part1_clipped_cleaned1.csv",
    "output/cleaned_trajs/lkpg_part2_clipped_cleaned1.csv",
    "output/cleaned_trajs/lkpg_part3_clipped_cleaned1.csv",
]
START_TIMES = [ # ORIGINAL START_TIMES
    "2022-03-22 07:15:58.5",
    "2022-03-22 07:37:26.8",
    "2022-03-22 08:11:16",
]
INPUT_COLUMN = "time_sec" # Offset from video start
INPUT_UNIT = "s" # Unit of INPUT_COLUMN values (pandas to_timedelta unit)
VIDEO_SPEED = 4 # Speed of the video relative to real time (e.g., 4 means 4x faster than real time)
OUTPUT_COLUMN = "timestamp" # Unix timestamp in seconds
TIMEZONE = "Europe/Stockholm" # Timezone of START_TIMES
REPLACE_ORIGINAL = True # If False, writes "<name>_timestamps.csv" next to the original


def add_timestamps(path: Path, start_time: str) -> Path:
    df = pd.read_csv(path)
    start_unix = pd.Timestamp(start_time).tz_localize(TIMEZONE).timestamp()
    offsets_s = pd.to_timedelta(df[INPUT_COLUMN], unit=INPUT_UNIT).dt.total_seconds()
    offsets_s *= VIDEO_SPEED  # Adjust for video speed
    df[OUTPUT_COLUMN] = start_unix + offsets_s

    out_path = path if REPLACE_ORIGINAL else path.with_name(f"{path.stem}_timestamps{path.suffix}")
    df.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    if len(INPUT_FILES) != len(START_TIMES):
        raise ValueError("INPUT_FILES and START_TIMES must have the same length")

    for file, start_time in zip(INPUT_FILES, START_TIMES):
        out_path = add_timestamps(Path(file), start_time)
        print(f"{file} -> {out_path}")


if __name__ == "__main__":
    main()
