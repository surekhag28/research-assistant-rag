from datetime import datetime
from typing import List, Optional
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session
from src.models.paper import Paper  # ORM object
from src.schemas.arxiv.paper import PaperCreate  # schema object


class PaperRepository:
    def __init__(self, session: Session):
        self.session = session

    def create(self, paper: PaperCreate) -> Paper:
        db_paper = Paper(**paper.model_dump())
        self.session.add(db_paper)
        self.session.commit()
        self.session.refresh(db_paper)
        return db_paper

    def get_by_arxiv_id(self, arxiv_id: str) -> Optional[Paper]:
        stmt = select(Paper).where(Paper.arxiv_id == arxiv_id)
        return self.session.scalar(stmt)

    def get_by_id(self, paper_id: UUID) -> Optional[Paper]:
        stmt = select(Paper).where(Paper.id == paper_id)
        return self.session.scalar(stmt)

    def get_all(self, limit: int = 100, offset: int = 0) -> List[Paper]:
        stmt = (
            select(Paper)
            .order_by(Paper.published_date.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(self.session.scalars(stmt))

    def get_count(self) -> int:
        stmt = select(func.count(Paper.id))
        return self.session.scalar(stmt) or 0

    def get_processed_papers(self, limit: int = 100, offset: int = 0) -> List[Paper]:
        stmt = (
            select(Paper)
            .where(Paper.pdf_processed == True)
            .order_by(Paper.pdf_processing_date.desc())
            .limit(limit)
            .offset(offset)
        )

        return list(self.session.scalars(stmt))

    def get_unprocessed_papers(self, limit: int = 100, offset: int = 0) -> List[Paper]:
        stmt = (
            select(Paper)
            .where(Paper.pdf_processed == False)
            .order_by(Paper.pdf_processing_date.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(self.session.scalars(stmt))

    def get_papers_with_raw_text(
        self, limit: int = 100, offset: int = 0
    ) -> List[Paper]:
        stmt = (
            select(Paper)
            .where(Paper.raw_text != None)
            .order_by(Paper.pdf_processing_date.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(stmt.session.scalars(stmt))

    def get_processing_stats(self) -> dict:
        total_count = self.get_count()

        # count processed papers
        processed_stats = select(func.count(Paper.id)).where(
            Paper.pdf_processed == True
        )
        processed_papers = self.session.scalar(processed_stats) or 0

        # count papers with text
        text_stmt = select(func.count(Paper.id)).where(Paper.raw_text != None)
        papers_with_text = self.session.scalar(text_stmt) or 0

        return {
            "total_papers": total_count,
            "processed_papers": processed_papers,
            "papers_with_text": papers_with_text,
            "processing_rate": (
                (processed_papers / total_count * 100) if total_count > 0 else 0
            ),
            "text_extraction_rate": (
                (papers_with_text / processed_papers * 100)
                if processed_papers > 0
                else 0
            ),
        }

    def update(self, paper: Paper) -> Paper:
        self.session.add(paper)
        self.session.commit()
        self.sessio.refresh(paper)
        return paper

    def upsert(self, paper_create: PaperCreate) -> Paper:
        existing_paper = self.get_by_arxiv_id(Paper.arxiv_id)
        if existing_paper:
            for key, value in paper_create.model_dump(exclude_unset=True):
                setattr(existing_paper, key, value)
            return self.update(existing_paper)
        else:
            return self.create(paper_create)
