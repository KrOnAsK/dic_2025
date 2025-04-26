#!/usr/bin/env python3
"""
Batch preprocessor for MapReduce input preparation.
This script can handle large files by processing them in chunks, which is useful
for the 56GB full dataset mentioned in the assignment.
"""

import json
import sys
import re
import argparse
import os
from typing import List, Set, Dict, Any
from pathlib import Path
from tqdm import tqdm  # For progress bar
import time
import sys
import argparse


class TextPreprocessor:
    """
    Handles the preprocessing steps for the text processing assignment:
    1. Tokenization to unigrams
    2. Case folding
    3. Stopword filtering
    """

    def __init__(self, stopwords_path: str):
        """
        Initialize the preprocessor with a path to the stopwords file.
        
        Args:
            stopwords_path (str): Path to the stopwords file
        """
        self.stopwords = self._load_stopwords(stopwords_path)
        # Compile regex pattern for tokenization
        # Using whitespaces, tabs, digits, and the characters ()[]{}.!?,;:+=-_"'`~#@&*%€$§\/
        self.tokenize_pattern = re.compile(r'[\s\t\d\(\)\[\]\{\}\.\!\?\,\;\:\+\=\-\_\"\'`\~\#\@\&\*\%\€\$\§\\\/]+')

    def _load_stopwords(self, path: str) -> Set[str]:
        """
        Load stopwords from a file.
        
        Args:
            path (str): Path to the stopwords file
        
        Returns:
            set: Set of stopwords for efficient lookup
        """
        stopwords = set()
        try:
            with open(path, 'r', encoding='utf-8') as f:
                for line in f:
                    word = line.strip()
                    if word:  # Add non-empty words
                        stopwords.add(word)
            print(f"Loaded {len(stopwords)} stopwords from {path}")
        except Exception as e:
            print(f"Error loading stopwords from {path}: {e}")
        return stopwords

    def tokenize(self, text: str) -> List[str]:
        """
        Tokenize text according to the assignment specifications:
        1. Convert to lowercase (case folding)
        2. Split on delimiters
        3. Filter out tokens with only one character
        4. Filter out stopwords
        
        Args:
            text (str): Input text to tokenize
            
        Returns:
            list: List of processed tokens
        """
        if not text:
            return []
        
        # Convert to lowercase (case folding)
        text = text.lower()
        
        # Split on delimiters and filter
        tokens = [
            token for token in self.tokenize_pattern.split(text)
            if token and len(token) > 1 and token not in self.stopwords
        ]
        
        return tokens


def preprocess_chunk(preprocessor, input_file, output_file, chunk_size=10000):
    """
    Process a large file in chunks to avoid memory issues.
    
    Args:
        preprocessor: Instance of TextPreprocessor
        input_file: Path to input file
        output_file: Path to output file
        chunk_size: Number of lines to process at once
    
    Returns:
        tuple: (processed_count, error_count)
    """
    processed_count = 0
    error_count = 0
    
    # Get total line count for progress bar
    total_lines = sum(1 for _ in open(input_file, 'r', encoding='utf-8'))
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        # Use tqdm for progress bar
        for line in tqdm(infile, total=total_lines, desc="Processing reviews"):
            try:
                # Parse the JSON review
                review = json.loads(line.strip())
                
                # Get the review text
                review_text = review.get('reviewText', '')
                
                # Apply preprocessing
                tokens = preprocessor.tokenize(review_text)
                
                # Create a new simplified JSON with just the needed fields
                processed_review = {
                    'reviewID': review.get('reviewerID', '') + "_" + review.get('asin', ''),
                    'category': review.get('category', ''),
                    'tokens': tokens
                }
                
                # Write the processed review to the output file
                outfile.write(json.dumps(processed_review) + '\n')
                
                processed_count += 1
                
            except json.JSONDecodeError:
                error_count += 1
            except Exception as e:
                print(f"Error processing review: {e}")
                error_count += 1
    
    return processed_count, error_count


def preprocess_reviews(input_path: str, output_path: str, stopwords_path: str):
    """
    Preprocess reviews from input file and write to output file.
    
    Args:
        input_path (str): Path to input JSON file with reviews (one per line)
        output_path (str): Path to output JSON file with preprocessed reviews
        stopwords_path (str): Path to stopwords file
    """
    start_time = time.time()
    # Initialize preprocessor
    preprocessor = TextPreprocessor(stopwords_path)
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Process the file in chunks
    processed_count, error_count = preprocess_chunk(preprocessor, input_path, output_path)

    
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Preprocessing completed in {execution_time:.2f} seconds")
    print(f"Processed {processed_count} reviews with {error_count} errors")


if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Preprocess Amazon review text data.')
    parser.add_argument('stopwords', help='Path to stopwords file')
    parser.add_argument('input', help='Path to input reviews JSON file')
    parser.add_argument('output', help='Path to output preprocessed JSON file')
    
    args = parser.parse_args()
    
    # Run the preprocessing
    preprocess_reviews(args.input, args.output, args.stopwords)
    start_time = time.time()
    preprocess_reviews(args.input, args.output, args.stopwords)
    end_time = time.time()
    print(f"Total script execution time: {end_time - start_time:.2f} seconds")