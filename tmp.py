import subprocess

def upload_file(file_path: str) -> str:
    identifier = "compiler-design-batch-2026-superft"

    cmd = f'''ia upload {identifier} "{file_path}" --metadata="title:Compiler Design Lecture 01" --metadata="mediatype:movies" --no-derive --retries=1 --checksum'''

    process = subprocess.Popen(cmd, shell=True)
    process.wait()

    return identifier


# Example usage
if __name__ == "__main__":
    path = "/workspaces/pw/compilerdesign_CH_01__Introduction_and_Lexical_Analysis_Introduction_and_Lexical_Analysis_05__Lexical_Analysis_Part_03.mp4"
    
    item_id = upload_file(path)
    print("Identifier:", item_id)
