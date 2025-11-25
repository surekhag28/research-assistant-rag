from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ParserType(str, Enum):
    """PDF parser types."""

    DOCLING = "docling"
    GROBID = "grobid"


class PaperSection(BaseModel):
    title: str = Field(..., description="Section title")
    content: str = Field(..., description="Section content")
    level: int = Field(default=1, description="Section hierarchy level")


class PaperFigure(BaseModel):
    caption: str = Field(..., description="Figure caption")
    id: str = Field(..., description="Figure identifier")


class PaperTable(BaseModel):
    caption: str = Field(..., description="Table caption")
    id: str = Field(..., description="Table identifier")


class PdfContent(BaseModel):
    sections: List[PaperSection] = Field(
        default_factory=list, description="Paper sections"
    )
    figures: List[PaperFigure] = Field(default_factory=list, description="Figures")
    tables: List[PaperTable] = Field(default_factory=list, description="Tables")
    raw_text: str = Field(..., description="Full extracted text")
    references: List[str] = Field(default_factory=list, description="References")
    parser_used: ParserType = Field(..., description="Parser used for extraction")
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Parser metadata"
    )


class ArxivMetadata(BaseModel):
    """Paper metadata from arxiv API."""

    title: str = Field(..., description="Paper title from arxiv")
    authors: List[str] = Field(..., description="Authors from arxiv")
    abstract: str = Field(..., description="Abstract from arxiv")
    arxiv_id: str = Field(..., description="arxiv identifier")
    categories: List[str] = Field(default_factory=list, description="arxiv categories")
    published_date: str = Field(..., description="Publication date")
    pdf_url: str = Field(..., description="PDF download URL")


class ParsedPaper(BaseModel):

    arxiv_metadata: ArxivMetadata = Field(..., description="Metadata from arxiv API")
    pdf_content: Optional[PdfContent] = Field(
        None, description="Content extracted from PDF"
    )
