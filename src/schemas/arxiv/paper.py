from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class ArxivPaper(BaseModel):
    """Schema for arxiv API response metadata data"""

    arxiv_id: str = Field(..., description="arxiv paper ID")
    title: str = Field(..., description="Paper title")
    authors: List[str] = Field(..., description="List of author names")
    abstract: str = Field(..., description="Paper abstract")
    categories: List[str] = Field(..., description="Paper categories")
    published_date: str = Field(..., description="Date published on arxiv (ISO format)")
    pdf_url: str = Field(..., description="URL to PDF")


class PaperBase(BaseModel):
    """Schema for arxiv API response metadata data"""

    arxiv_id: str = Field(..., description="arxiv paper ID")
    title: str = Field(..., description="Paper title")
    authors: List[str] = Field(..., description="List of author names")
    abstract: str = Field(..., description="Paper abstract")
    categories: List[str] = Field(..., description="Paper categories")
    published_date: str = Field(..., description="Date published on arxiv (ISO format)")
    pdf_url: str = Field(..., description="URL to PDF")


class PaperCreate(PaperBase):
    raw_text: Optional[str] = Field(
        None, description="Full raw text extracted from PDF"
    )
    sections: Optional[List[Dict[str, Any]]] = Field(
        None, description="List of sections with title and content"
    )
    references: Optional[List[Dict[str, Any]]] = Field(
        None, description="List of references if extracted"
    )
    parser_used: Optional[str] = Field(
        None, description="Which parser was used (DOCLING, GORBID etc.)"
    )
    parser_metdata: Optional[Dict[str, Any]] = Field(
        None, description="Additional parser metadata"
    )
    pdf_processed: Optional[bool] = Field(
        False, description="Whether PDF was successfully processed"
    )
    pdf_processing_date: Optional[datetime] = Field(
        None, description="When PDF was processed"
    )
