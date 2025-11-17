from typing import List
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
