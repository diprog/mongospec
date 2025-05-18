from .base import BaseOperations, TDocument


class InsertOperationsMixin(BaseOperations):
    """Mixin class for insert operations"""

    @classmethod
    async def insert_one(cls: type[TDocument], document: TDocument) -> TDocument:
        """
        Insert single document into collection

        :param document: Document instance to insert
        :return: Inserted document with _id
        """
        cls._validate_document_type(document)
        result = await cls._get_collection().insert_one(document.dump())
        document._id = result["inserted_id"]
        return document

    async def insert(self: TDocument) -> TDocument:
        """
        Insert the document instance into collection including existing _id

        :return: Inserted document with _id (newly generated or existing)
        """
        self._validate_document_type(self)
        result = await self._get_collection().insert_one(self.dump())
        self._id = result["inserted_id"]
        return self

    @classmethod
    async def insert_many(cls: type[TDocument], documents: list[TDocument]) -> list[TDocument]:
        """
        Batch insert documents

        :param documents: List of document instances
        :return: List of inserted documents with _ids
        """
        if not all(isinstance(d, cls) for d in documents):
            raise TypeError("All documents must match collection type")

        result = await cls._get_collection().insert_many([d.dump() for d in documents])

        for doc, doc_id in zip(documents, result["inserted_ids"]):
            doc._id = doc_id
        return documents
