import json
import os
import time
from pathlib import Path
from typing import *

import httpx
import requests
from dotenv import load_dotenv
from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings
from mineru.cli import api_client as _api_client
from mineru.cli.common import image_suffixes, office_suffixes, pdf_suffixes
from mineru.utils.guess_suffix_or_lang import guess_suffix_by_path
from pydantic import ValidationError

load_dotenv()


class Settings:
    UNST_API_KEY: str | None = os.getenv("UNSTRUCTURED_API_KEY")
    UNST_API_URL: str | None = os.getenv("UNSTRUCTURED_API_URL")
    VECTORSTORE_DIR = Path("src/data/minio")
    MINERU_HOST: str = "http://127.0.0.0:8000"
    MINIO_HOST: str = "http://127.0.0.0:9000"
    QDRANT_HOST: str = "http://127.0.0.0:6333"
    SUPPORTED_INPUT_SUFFIXES = set(pdf_suffixes + image_suffixes + office_suffixes)


class MineruClient:
    def __init__(self) -> None:
        self.url = Settings.MINERU_HOST

    def _iter_input_files(self, input_path: str | Path) -> List[Path]:
        p = Path(input_path)

        if p.is_file():
            return [p]

        if p.is_dir():
            return [
                f
                for f in p.iterdir()
                if f.suffix.lower() in Settings.SUPPORTED_INPUT_SUFFIXES
            ]

    def partition(self, path: str):
        files_paths = self._iter_input_files(path)
        if not files_paths:
            raise ValueError(f"No input files found under {path}")

        req_data = {
            "files": files_paths,
            "lang_list": "east_slavic",
            "backend": "pipeline",
            "parse_method": "auto",
            "formula_enable": True,
            "table_enable": True,
            "return_md": True,
            "return_middle_json": False,
            "return_model_output": False,
            "return_content_list": False,
            "return_images": True,
        }

        response = requests.post(self.url + "/tasks", req_data)
        js = response.json()

        if response.status_code == 202:
            return js.get("task_id")
        elif response.status_code == 422:
            raise ValidationError(f"{js}")
        else:
            response.raise_for_status()


class UnstructuredClient:
    def __init__(self, api_key: str, api_url: str):
        self.api_key = api_key
        self.api_url = api_url
        self.vectorstore_dir = Settings.VECTORSTORE_DIR

    def partition(self, filepath: str) -> list[dict]:
        with open(filepath, "rb") as f:
            file_content = f.read()

        headers = {"UNSTRUCTURED-API-KEY": self.api_key}

        file_ext = Path(filepath).suffix.lower()

        if file_ext == ".md":
            data = {"strategy": "auto"}
        else:
            data = {
                "strategy": "hi_res",
                "languages": ["rus", "eng"],
                "infer_table_structure": True,
            }

        files = {"files": (Path(filepath).name, file_content)}

        print(f"  Parsing: {Path(filepath).name}...")
        start = time.perf_counter()

        resp = httpx.post(
            self.api_url,
            headers=headers,
            data=data,
            files=files,
            timeout=None,
        )

        elapsed = time.perf_counter() - start
        print(f"  Done in {elapsed:.1f}s")

        resp.raise_for_status()
        return resp.json()


class VectorStoreService:
    def __init__(self):
        self.embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

    def save(self, documents: list, persist_dir: str):
        Chroma.from_documents(
            documents=documents,
            embedding=self.embeddings,
            persist_directory=persist_dir,
        )

    def load(self, persist_dir: str) -> Chroma:
        return Chroma(
            persist_directory=persist_dir,
            embedding_function=self.embeddings,
        )


class JSONStorage:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save(self, data: list, name: str):
        path = self.base_dir / f"{name}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def load(self, name: str) -> list | None:
        path = self.base_dir / f"{name}.json"
        print(path)
        if not path.exists():
            return None

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


class DocumentParser:
    def __init__(self, client, storage, vectorstore):
        self.client = client
        self.storage = storage
        self.vectorstore = vectorstore
        self.vectorstore_dir = Settings.VECTORSTORE_DIR

    def _elements_to_documents(
        self, elements: list[dict], source_file: str
    ) -> list[Document]:
        docs = []

        for el in elements:
            metadata = {
                "source": source_file,
                "category": el.get("type", "Unknown"),
            }

            page = el.get("metadata", {}).get("page_number")
            if page:
                metadata["page_number"] = page

            docs.append(
                Document(
                    page_content=el.get("text", ""),
                    metadata=metadata,
                )
            )

        return docs

    def single_parse(self, filepath: str) -> list[Document]:
        source_name = Path(filepath).stem
        print(source_name)
        persist_dir = str(self.vectorstore_dir / source_name)

        # 1. Cache
        elements = self.storage.load(source_name)

        # 2. API
        if not elements:
            elements = self.client.partition(filepath)
            self.storage.save(elements, source_name)

        # 3. transform
        docs = self._elements_to_documents(elements, source_file=Path(filepath).name)

        # 4. vectorstore
        if not os.path.exists(persist_dir):
            self.vectorstore.save(docs, persist_dir)

        return docs


def main():
    assert Settings.UNST_API_KEY is not None, "UNST_API_KEY must be set"
    assert Settings.UNST_API_URL is not None, "UNST_API_URL must be set"

    client = UnstructuredClient(
        Settings.UNST_API_KEY,
        Settings.UNST_API_URL,
    )

    storage = JSONStorage(Path("src/data/unstruct_json"))

    vectorstore = VectorStoreService()

    parser = DocumentParser(client, storage, vectorstore)

    parser.single_parse(
        "src/data/mineru_data/Пособие/Пособие по проектированию бетонных и железобетонных конструкций.md"
    )


if __name__ == "__main__":
    main()
