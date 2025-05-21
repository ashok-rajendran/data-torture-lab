CREATE OR REPLACE TABLE friends (
    PersonID INTEGER NOT NULL,
    FriendID INTEGER NOT NULL
);

INSERT INTO friends (PersonID, FriendID) VALUES
(1, 2),
(1, 3),
(2, 1),
(2, 3),
(3, 5),
(4, 2),
(4, 3),
(4, 5);

CREATE OR REPLACE TABLE persons (
    PersonID INTEGER NOT NULL,
    Name STRING NOT NULL,
    Email STRING NOT NULL,
    Score INTEGER,
    PRIMARY KEY (PersonID)
);

INSERT INTO persons (PersonID, Name, Email, Score) VALUES
(1, 'Alice', 'alice2018@hotmail.com', 88),
(2, 'Bob', 'bob2018@hotmail.com', 11),
(3, 'Davis', 'davis2018@hotmail.com', 27),
(4, 'Tara', 'tara2018@hotmail.com', 45),
(5, 'John', 'john2018@hotmail.com', 63);

with score_stats as 
(select 
f.personid, 
sum(p.score) as total_friends_score,
count(p.personid) as total_friends
from friends f 
inner join persons p 
on f.friendid = p.personid
group by f.personid
having sum(p.score)>100
)
select 
s.*,p.name 
from score_stats s inner join persons p 
on s.personid = p.personid
;
