CREATE TABLE Badges (
  Id SERIAL PRIMARY KEY,
  UserId integer NOT NULL,
  Name varchar(50) NOT NULL,
  Date timestamp NOT NULL,
  Class smallint NOT NULL,
  TagBased boolean NOT NULL
);
CREATE TABLE Comments (
  Id SERIAL PRIMARY KEY,
  PostId integer NOT NULL,
  Score integer NOT NULL,
  Text varchar(600) NOT NULL,
  CreationDate timestamp NOT NULL,
  UserId integer,
  UserDisplayName varchar(30)
);
CREATE TABLE PostHistory (
  Id SERIAL PRIMARY KEY,
  PostHistoryTypeId smallint NOT NULL,
  PostId integer,
  RevisionGUID uuid NOT NULL,
  CreationDate timestamp NOT NULL,
  UserId integer,
  Text text,
  ContentLicense varchar,
  Comment varchar(400),
  UserDisplayName varchar(40)
);
CREATE TABLE PostLinks (
  Id SERIAL PRIMARY KEY,
  CreationDate timestamp NOT NULL,
  PostId integer NOT NULL,
  RelatedPostId integer,
  LinkTypeId smallint NOT NULL
);
CREATE TABLE Posts (
  OriginalId INTEGER NOT NULL,
  ArchiveVersion SMALLINT NOT NULL,
  Id SERIAL PRIMARY KEY,
  PostTypeId smallint NOT NULL,
  CreationDate timestamp NOT NULL,
  Score integer NOT NULL,
  ViewCount integer,
  Body text,
  OwnerUserId integer,
  LastActivityDate timestamp,
  Title varchar(250),
  Tags varchar(250),
  AnswerCount integer,
  CommentCount integer,
  ContentLicense varchar,
  ParentId integer,
  LastEditorUserId integer,
  LastEditDate timestamp,
  AcceptedAnswerId integer,
  ClosedDate timestamp,
  LastEditorDisplayName varchar(40),
  CommunityOwnedDate timestamp,
  FavoriteCount integer,
  OwnerDisplayName varchar(40)
);
CREATE TABLE Tags (
  Id SERIAL PRIMARY KEY,
  TagName varchar(35),
  Count integer NOT NULL,
  IsRequired boolean,
  ExcerptPostId integer,
  WikiPostId integer,
  IsModeratorOnly boolean
);
CREATE TABLE Users (
  OriginalId INTEGER NOT NULL,
  ArchiveVersion SMALLINT NOT NULL,
  Id SERIAL PRIMARY KEY,
  Reputation integer NOT NULL,
  CreationDate timestamp NOT NULL,
  DisplayName varchar(40),
  LastAccessDate timestamp NOT NULL,
  WebsiteUrl varchar(200),
  Location varchar(100),
  AboutMe text,
  Views integer NOT NULL,
  UpVotes integer NOT NULL,
  DownVotes integer NOT NULL,
  AccountId integer
);
CREATE TABLE Votes (
  Id SERIAL PRIMARY KEY,
  PostId integer,
  VoteTypeId smallint NOT NULL,
  CreationDate timestamp,
  UserId integer,
  BountyAmount integer
);
