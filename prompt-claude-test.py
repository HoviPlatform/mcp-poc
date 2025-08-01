import os
import requests

# Load API key from environment variable (recommended)
ANTHROPIC_API_KEY = <>
if not ANTHROPIC_API_KEY:
    raise ValueError("Please set the ANTHROPIC_API_KEY environment variable.")

# Load the prompt from prompt.txt
try:
    with open("prompt.txt", "r", encoding="utf-8") as file:
        user_prompt = file.read().strip()
except FileNotFoundError:
    print("Error: prompt.txt file not found. Please create this file with your prompt.")
    exit(1)
except Exception as e:
    print(f"Error reading prompt.txt: {e}")
    exit(1)

if not user_prompt:
    print("Warning: prompt.txt is empty")
    exit(1)

API_URL = "https://api.anthropic.com/v1/messages"
HEADERS = {
    "x-api-key": ANTHROPIC_API_KEY,
    "anthropic-version": "2023-06-01",
    "content-type": "application/json"
}

# Define the payload with the prompt
data = {
    "model": "claude-sonnet-4-20250514",  # You can switch to claude-3-sonnet or claude-3-haiku
    "max_tokens": 4096,  # Increased for longer responses
    "temperature": 0.7,
    "messages": [
        {"role": "user", "content": user_prompt}
    ]
}

print("Sending request to Claude API...")
print(f"Prompt length: {len(user_prompt)} characters")
print("-" * 50)

# Send the request
try:
    response = requests.post(API_URL, headers=HEADERS, json=data, timeout=60)
    
    # Print the result
    if response.status_code == 200:
        result = response.json()
        
        # Print the entire response
        print("\n=== Claude Response ===\n")
        
        # Handle multiple content blocks if present
        if "content" in result and isinstance(result["content"], list):
            full_response = ""
            for content_block in result["content"]:
                if content_block.get("type") == "text":
                    full_response += content_block.get("text", "")
            
            print(full_response)
            
            # Print usage stats if available
            if "usage" in result:
                usage = result["usage"]
                print(f"\n=== Usage Stats ===")
                print(f"Input tokens: {usage.get('input_tokens', 'N/A')}")
                print(f"Output tokens: {usage.get('output_tokens', 'N/A')}")
                print(f"Total tokens: {usage.get('input_tokens', 0) + usage.get('output_tokens', 0)}")
        else:
            print("Unexpected response format")
            print("Full response:", result)
            
    else:
        print(f"Error {response.status_code}: {response.text}")
        
        # Try to parse error details
        try:
            error_data = response.json()
            if "error" in error_data:
                print(f"Error type: {error_data['error'].get('type', 'Unknown')}")
                print(f"Error message: {error_data['error'].get('message', 'Unknown')}")
        except:
            pass

except requests.exceptions.Timeout:
    print("Request timed out. The API might be taking longer than expected.")
except requests.exceptions.RequestException as e:
    print(f"Request error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")