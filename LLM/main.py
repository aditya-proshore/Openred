import vertexai
from vertexai.generative_models import GenerativeModel
from google.oauth2 import service_account
import json
def get_project_id_from_file(service_account_path: str) -> str:
    """Extracts the project_id from the service account JSON file."""
    try:
        with open(service_account_path, "r") as f:
            data = json.load(f)
            project_id = data.get("project_id")
            if not project_id:
                raise ValueError("Key 'project_id' not found in service account file.")
            return project_id
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Service account file not found: {service_account_path}"
        )
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        raise RuntimeError(f"Error reading project_id from {service_account_path}: {e}")
def generate_text_vertexai(
    project_id: str,
    location: str,
    credentials,
    prompt: str,
    model_name: str = "gemini-2.5-flash",
) -> str:
    print(f"Initializing Vertex AI for project: {project_id}, location: {location}")
    try:
        vertexai.init(project=project_id, location=location, credentials=credentials)
        print(f"Loading model: {model_name}")
        model = GenerativeModel(model_name)
        print(f"Sending prompt: '{prompt[:50]}...'")
        response = model.generate_content(prompt)
        print("Response received.")
        if response.candidates and response.candidates[0].content.parts:
            return response.candidates[0].content.parts[0].text
        else:
            print(f"Warning: No valid response candidates found. Response: {response}")
            return ""
    except Exception as e:
        print(f"An error occurred during Vertex AI text generation: {e}")
        return ""
if __name__ == "__main__":
    try:
        SERVICE_ACCOUNT_FILE = (
            "/home/aditya/Documents/Veneficus/data-processing/zyte/key.json"
        )
        LOCATION = "europe-west4"
        MODEL_NAME = "gemini-2.5-flash"
        with open("prompt.txt", "r") as f:
            USER_PROMPT = f.read()
        with open("document.txt", "r") as f:
            document_text = f.read().strip()
        USER_PROMPT = (f"{USER_PROMPT} Text={document_text}")
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE
        )
        proj_id = get_project_id_from_file(SERVICE_ACCOUNT_FILE)
        generated_text = generate_text_vertexai(
            project_id=proj_id,
            location=LOCATION,
            credentials=creds,
            prompt=USER_PROMPT,
            model_name=MODEL_NAME,
        )
        if generated_text:
            print("\n--- Generated Text ---")
            print(generated_text)
        else:
            print("\nFailed to generate text or received an empty response.")
    except (FileNotFoundError, RuntimeError, Exception) as e:
        print(f"\nAn critical error occurred: {e}")
