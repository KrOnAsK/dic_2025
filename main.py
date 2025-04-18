#!/usr/bin/env python3
"""
Pipeline runner script that:
1. Executes the preprocessing.py script
2. Executes the chisquared.py script
3. Tracks and displays execution time for each step and the overall process

 Run this script from the command line with the following arguments:
    
        --preprocessing <path_to_preprocessing.py>
        --chisquared <path_to_chisquared.py>
        --stopwords <path_to_stopwords_file>
        --reviews <path_to_reviews_file>
        [--preprocessed <path_to_preprocessed_file>]
        [--output <path_to_output_file>]
        [--skip-preprocessing]
Example: python main.py --preprocessing preprocessing.py --chisquared chisquared.py --stopwords stopwords.txt --reviews reviews_devset.json 
"""

import subprocess
import argparse
import time
import os
import sys


def run_preprocessing(preprocessing_script, stopwords_path, input_path, output_path):
    """
    Run the preprocessing script as a subprocess and measure execution time.
    
    Args:
        preprocessing_script (str): Path to the preprocessing.py script
        stopwords_path (str): Path to the stopwords file
        input_path (str): Path to the input reviews file
        output_path (str): Path to save the preprocessed output
        
    Returns:
        float: Execution time in seconds
        int: Return code (0 for success)
    """
    print(f"Running preprocessing on {input_path}...")
    start_time = time.time()
    
    process = subprocess.run(
        ['python', preprocessing_script, stopwords_path, input_path, output_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(process.stdout)
    if process.stderr:
        print("Errors during preprocessing:")
        print(process.stderr)
    
    return execution_time, process.returncode


def run_chi_squared(chisquared_script, input_path, output_path):
    """
    Run the chi-squared calculation script as a subprocess and measure execution time.
    
    Args:
        chisquared_script (str): Path to the chisquared.py script
        input_path (str): Path to the preprocessed JSON file
        output_path (str): Path to save the output results
        
    Returns:
        float: Execution time in seconds
        int: Return code (0 for success)
    """
    print(f"Running chi-squared calculation on {input_path}...")
    start_time = time.time()
    
    # Run the chi-squared script
    process = subprocess.run(
        ['python', chisquared_script, input_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    
    # Write output to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(process.stdout)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    if process.stderr:
        print("Errors during chi-squared calculation:")
        print(process.stderr)
    
    return execution_time, process.returncode


def main():
    """
    Main function that parses arguments and runs the pipeline.
    """
    parser = argparse.ArgumentParser(description='Run the complete Amazon review processing pipeline.')
    
    # Required arguments
    parser.add_argument('--preprocessing', required=True, help='Path to the preprocessing.py script')
    parser.add_argument('--chisquared', required=True, help='Path to the chisquared.py script')
    parser.add_argument('--stopwords', required=True, help='Path to the stopwords file')
    parser.add_argument('--reviews', required=True, help='Path to the input reviews JSON file')
    
    # Optional arguments
    parser.add_argument('--preprocessed', help='Path to save the preprocessed reviews (default: preprocessed_reviews.json)')
    parser.add_argument('--output', help='Path to save the chi-squared results (default: output.txt)')
    parser.add_argument('--skip-preprocessing', action='store_true', help='Skip preprocessing and use existing preprocessed file')
    
    args = parser.parse_args()
    
    # Set default paths if not provided
    preprocessed_path = args.preprocessed or 'preprocessed_reviews.json'
    output_path = args.output or 'output.txt'
    
    overall_start_time = time.time()
    preprocessing_time = 0
    
    # Check if scripts exist
    if not os.path.exists(args.preprocessing):
        print(f"Error: Preprocessing script not found at {args.preprocessing}")
        return 1
    
    if not os.path.exists(args.chisquared):
        print(f"Error: Chi-squared script not found at {args.chisquared}")
        return 1
    
    # Run preprocessing if not skipped
    if not args.skip_preprocessing:
        preprocessing_time, return_code = run_preprocessing(
            args.preprocessing, args.stopwords, args.reviews, preprocessed_path
        )
        
        if return_code != 0:
            print(f"Preprocessing failed with return code {return_code}")
            return return_code
        
        print(f"Preprocessing completed in {preprocessing_time:.2f} seconds")
    else:
        print(f"Skipping preprocessing, using existing file: {preprocessed_path}")
        
        if not os.path.exists(preprocessed_path):
            print(f"Error: Preprocessed file {preprocessed_path} does not exist")
            return 1
    
    # Run chi-squared calculation
    chisquared_time, return_code = run_chi_squared(
        args.chisquared, preprocessed_path, output_path
    )
    
    if return_code != 0:
        print(f"Chi-squared calculation failed with return code {return_code}")
        return return_code
    
    print(f"Chi-squared calculation completed in {chisquared_time:.2f} seconds")
    
    # Calculate overall time
    overall_end_time = time.time()
    overall_time = overall_end_time - overall_start_time
    
    # Print summary
    print("\nExecution Summary:")
    print("-----------------")
    if not args.skip_preprocessing:
        print(f"Preprocessing time: {preprocessing_time:.2f} seconds")
    print(f"Chi-squared calculation time: {chisquared_time:.2f} seconds")
    print(f"Total pipeline time: {overall_time:.2f} seconds")
    print(f"Results saved to: {output_path}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())