import os

import pandas as pd


def main():
    raw_dir = "/opt/ml/processing/input/raw"
    out_train_dir = "/opt/ml/processing/output/train"
    out_test_dir = "/opt/ml/processing/output/test"

    os.makedirs(out_train_dir, exist_ok=True)
    os.makedirs(out_test_dir, exist_ok=True)

    # Expect a single file named data.csv in raw_dir
    input_path = os.path.join(raw_dir, "data.csv")
    df = pd.read_csv(input_path)

    # Simple split 80/20
    train = df.sample(frac=0.8, random_state=42)
    test = df.drop(train.index)

    train_path = os.path.join(out_train_dir, "train.csv")
    test_path = os.path.join(out_test_dir, "test.csv")
    train.to_csv(train_path, index=False)
    test.to_csv(test_path, index=False)


if __name__ == "__main__":
    main()
