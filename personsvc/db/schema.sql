
CREATE TABLE "UserName" (
    Id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    FirstName VARCHAR(50) NOT NULL,
    MiddleName VARCHAR(50),
    LastName VARCHAR(100) NOT NULL,
    Suffix VARCHAR(20),
    CreatedDateTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedDateTime TIMESTAMP
);

CREATE TABLE "Outbox" (
    Id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    Topic VARCHAR(255) NOT NULL,
    EventName VARCHAR(255) NOT NULL,
    Payload TEXT NOT NULL,
    "Status" VARCHAR(100) NOT NULL DEFAULT 'Unpublished',
    CreatedDateTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PublishedDateTime TIMESTAMP,
    ErrorCount INT NOT NULL DEFAULT 0,
    ErrorMessage VARCHAR(255)
);

