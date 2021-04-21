USE [######]
GO
CREATE NONCLUSTERED INDEX [IX_MatchComparison1]
ON [dbo].[######] ([isFH])
INCLUDE ([FName], [MName], [LName], [DOB], [DOD], [CurrentState], [DateEntered])
GO;

USE [######]
GO
CREATE NONCLUSTERED INDEX [IX_MatchComparison2]
ON [dbo].[######] ([isFH])
INCLUDE ([FName], [MName], [LName], [DOB], [DOD], [CurrentState], [DateEntered])
GO;