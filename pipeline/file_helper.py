import os
from typing import List, Optional


class FileHelper:

    @staticmethod
    def load_text(file_path: str) -> Optional[str]:
        """
        Load text content from a file.

        Args:
            file_path: Path to the file to read

        Returns:
            File content as string, or None if file cannot be read

        Raises:
            FileNotFoundError: If the file doesn't exist
            PermissionError: If file access is denied
            UnicodeDecodeError: If file encoding is invalid
        """
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")

            with open(file_path, "r", encoding="utf-8") as f:
                text = f.read()
            return text

        except FileNotFoundError:
            raise
        except PermissionError:
            raise PermissionError(f"Permission denied accessing file: {file_path}")
        except Exception as e:
            raise Exception(f"Unexpected error reading file {file_path}: {e}")

    @staticmethod
    def load_lines(file_path: str) -> List[str]:
        """
        Load file content as a list of lines.

        Args:
            file_path: Path to the file to read

        Returns:
            List of lines from the file

        Raises:
            FileNotFoundError: If the file doesn't exist
            PermissionError: If file access is denied
            UnicodeDecodeError: If file encoding is invalid
        """
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")

            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            return [line.strip() for line in lines]

        except FileNotFoundError:
            raise
        except PermissionError:
            raise PermissionError(f"Permission denied accessing file: {file_path}")
        except Exception as e:
            raise Exception(f"Unexpected error reading file {file_path}: {e}")

    @staticmethod
    def write_text(file_path: str, text: str) -> None:
        """
        Write text content to a file.

        Args:
            file_path: Path to the file to write
            text: Text content to write

        Raises:
            PermissionError: If file access is denied
            OSError: If there are file system errors
        """
        try:
            # Ensure directory exists
            directory = os.path.dirname(file_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(text)

        except PermissionError:
            raise PermissionError(f"Permission denied writing to file: {file_path}")
        except OSError as e:
            raise OSError(f"File system error writing to {file_path}: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error writing to file {file_path}: {e}")
