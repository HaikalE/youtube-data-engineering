#!/usr/bin/env python3
import pickle
import os
import sys

def check_pickle_file(file_path):
    """Check content of a pickle file"""
    print(f"Checking {file_path}...")
    try:
        with open(file_path, 'rb') as f:
            data = pickle.load(f)
            
        if isinstance(data, dict):
            print(f"Data contains {len(data)} categories/keys")
            for key, value in data.items():
                if hasattr(value, 'shape'):
                    print(f"  Key {key}: DataFrame with shape {value.shape}")
                else:
                    print(f"  Key {key}: {type(value)}")
        else:
            print(f"Data type: {type(data)}")
            
    except Exception as e:
        print(f"Error loading {file_path}: {e}")

# Check both raw and processed data
data_dir = 'data'
raw_data_path = os.path.join(data_dir, 'raw_data.pkl')
processed_data_path = os.path.join(data_dir, 'processed_data.pkl')

if os.path.exists(raw_data_path):
    check_pickle_file(raw_data_path)
else:
    print(f"File {raw_data_path} does not exist")

print("\n" + "-"*50 + "\n")

if os.path.exists(processed_data_path):
    check_pickle_file(processed_data_path)
else:
    print(f"File {processed_data_path} does not exist")