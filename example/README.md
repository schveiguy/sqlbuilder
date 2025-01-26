# Simple example of how to use sqlbuilder

In this example, we imagine a database of books, authors, and reviews.

SQLBuilder uses *structs* as models, to determine how to manage data in the database. Thefore we need to model the database schema using structures, and identify how they are related using UDAs.

For our design:

- A Book will have a title and publication date. It will be related to a number of authors, and a number of reviews
- An Author will have first and last names
- A Review will have a username, a text review and a rating.

Then we will populate the data with all the D books, and make up some reviews.

And finally, we will run queries on that data based on a CLI.

The backend we will use is sqlite, since it's easy to use without having to install a server.
