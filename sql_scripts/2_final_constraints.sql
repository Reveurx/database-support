-- Обновляем AcceptedAnswerId
UPDATE Posts p
SET AcceptedAnswerId = sub.id
FROM (
    SELECT OriginalId, ArchiveVersion, Id
    FROM Posts
) sub
WHERE p.AcceptedAnswerId = sub.OriginalId
  AND p.ArchiveVersion = sub.ArchiveVersion;

-- Обновляем ParentId
UPDATE Posts p
SET ParentId = sub.id
FROM (
    SELECT OriginalId, ArchiveVersion, Id
    FROM Posts
) sub
WHERE p.ParentId = sub.OriginalId
  AND p.ArchiveVersion = sub.ArchiveVersion;

ALTER TABLE Badges ADD CONSTRAINT fk_Badges_UserId FOREIGN KEY (UserId) REFERENCES Users(Id);
ALTER TABLE Comments ADD CONSTRAINT fk_Comments_PostId FOREIGN KEY (PostId) REFERENCES Posts(Id);
ALTER TABLE Comments ADD CONSTRAINT fk_Comments_UserId FOREIGN KEY (UserId) REFERENCES Users(Id);
ALTER TABLE PostHistory ADD CONSTRAINT fk_PostHistory_PostId FOREIGN KEY (PostId) REFERENCES Posts(Id);
ALTER TABLE PostHistory ADD CONSTRAINT fk_PostHistory_UserId FOREIGN KEY (UserId) REFERENCES Users(Id);
ALTER TABLE PostLinks ADD CONSTRAINT fk_PostLinks_PostId FOREIGN KEY (PostId) REFERENCES Posts(Id);
ALTER TABLE PostLinks ADD CONSTRAINT fk_PostLinks_RelatedPostId FOREIGN KEY (RelatedPostId) REFERENCES Posts(Id);
ALTER TABLE Posts ADD CONSTRAINT fk_Posts_OwnerUserId FOREIGN KEY (OwnerUserId) REFERENCES Users(Id);
ALTER TABLE Posts ADD CONSTRAINT fk_Posts_ParentId FOREIGN KEY (ParentId) REFERENCES Posts(Id);
ALTER TABLE Posts ADD CONSTRAINT fk_Posts_LastEditorUserId FOREIGN KEY (LastEditorUserId) REFERENCES Users(Id);
ALTER TABLE Posts ADD CONSTRAINT fk_Posts_AcceptedAnswerId FOREIGN KEY (AcceptedAnswerId) REFERENCES Posts(Id);
ALTER TABLE Tags ADD CONSTRAINT fk_Tags_ExcerptPostId FOREIGN KEY (ExcerptPostId) REFERENCES Posts(Id);
ALTER TABLE Tags ADD CONSTRAINT fk_Tags_WikiPostId FOREIGN KEY (WikiPostId) REFERENCES Posts(Id);
ALTER TABLE Votes ADD CONSTRAINT fk_Votes_PostId FOREIGN KEY (PostId) REFERENCES Posts(Id);
ALTER TABLE Votes ADD CONSTRAINT fk_Votes_UserId FOREIGN KEY (UserId) REFERENCES Users(Id);

CREATE INDEX ON Badges(UserId);
CREATE INDEX ON Badges(Name);
CREATE INDEX ON Badges(Date);
CREATE INDEX ON Badges(Class);
CREATE INDEX ON Badges(TagBased);
CREATE INDEX ON Comments(PostId);
CREATE INDEX ON Comments(Score);
CREATE INDEX ON Comments(Text);
CREATE INDEX ON Comments(CreationDate);
CREATE INDEX ON PostHistory(PostHistoryTypeId);
CREATE INDEX ON PostHistory(PostId);
CREATE INDEX ON PostHistory(RevisionGUID);
CREATE INDEX ON PostHistory(CreationDate);
CREATE INDEX ON PostLinks(CreationDate);
CREATE INDEX ON PostLinks(PostId);
CREATE INDEX ON PostLinks(RelatedPostId);
CREATE INDEX ON PostLinks(LinkTypeId);
CREATE INDEX ON Posts(PostTypeId);
CREATE INDEX ON Posts(CreationDate);
CREATE INDEX ON Posts(Score);
CREATE INDEX ON Tags(Count);
CREATE INDEX ON Users(Reputation);
CREATE INDEX ON Users(CreationDate);
CREATE INDEX ON Users(LastAccessDate);
CREATE INDEX ON Users(Views);
CREATE INDEX ON Users(UpVotes);
CREATE INDEX ON Users(DownVotes);
CREATE INDEX ON Votes(PostId);
CREATE INDEX ON Votes(VoteTypeId);
