import std.stdio;
import sqlbuilder.dataset;
import sqlbuilder.uda;
import sqlbuilder.types;
import sqlbuilder.dialect.sqlite;
import d2sqlite3;
import std.datetime.date;

// Example of a custom serialized type from the database. This is a boolean
// value which is stored in the database as a string of "Y" or "N".
struct MyBool
{
    bool _val;
    alias _val this;

    // used to serialize to the database
    string dbValue() { return _val ? "Y" : "N"; }

    // used to deserialize from the database
    static MyBool fromDbValue(string item) {
        return MyBool(item == "Y" || item == "y");
    }
}

@tableName("author")
struct Author
{
    string firstName;
    string lastName;
    @primaryKey @autoIncrement int id = -1;

    // relations
    static @mapping("author_id") @refersTo!book Relation books;
    static @mapping("author_id") @mapping("book_type", "= 0") @refersTo!book Relation referenceBooks;
}

enum BookType {
    Reference,
    Fiction
}

struct book
{
    @unique @colName("name") @colType("VARCHAR(100)") string title;
    @refersTo!Author("author", Spec.innerJoin) @colName("auth_id") int author_id;
    Date published;
    BookType book_type;
    @primaryKey @autoIncrement int id = -1;
}

struct review
{
    @refersTo!book("book") int book_id;
    @allowNull string comment;
    @allowNull(-1) int rating;
    @colType("VARCHAR(1)") MyBool recommended;
}

enum datafilename = "books.sqlite";

int main(string[] args)
{
    if(args.length == 1)
    {
        // initialize the books
        writeln("Initializing database!");
        import std.file;
        if(exists(datafilename))
            remove(datafilename);
        auto db = Database(datafilename);
        // for sqlite, the `true` second parameter says to create all foreign
        // key constraints detected with the model.
        db.execute(createTableSql!(Author, true));
        db.execute(createTableSql!(book, true));
        db.execute(createTableSql!(review, true));

        // insert all the authors

        auto authors = [
            Author(
                    firstName: "Andrei",
                    lastName: "Alexandrescu"
                  ),
            Author(
                    firstName: "Mike",
                    lastName: "Parker"
                  ),
            Author(
                    firstName: "Ali",
                    lastName: "Çehreli"
                  ),
            Author(
                    firstName: "Charles",
                    lastName: "D-kins"
                  )
        ];


        foreach(ref author; authors)
            db.create(author);

        // create books
        auto books = [
            book(
                    title: "The D Programming Language",
                    author_id: authors[0].id,
                    book_type: BookType.Reference,
                    published: Date(2010, 6, 2)
                ),
            book(
                    title: "Modern C++ Design",
                    author_id: authors[0].id,
                    book_type: BookType.Reference,
                    published: Date(2001, 2, 13)
                ),
            book(
                    title: "Learning D",
                    author_id: authors[1].id,
                    book_type: BookType.Reference,
                    published: Date(2015, 11, 30)
                ),
            book(
                    title: "D Programming",
                    author_id: authors[2].id,
                    book_type: BookType.Reference,
                    published: Date(2015, 12, 11)
                ),
            book(
                    title: "A Tale of Two Languages",
                    author_id: authors[3].id,
                    book_type: BookType.Fiction,
                    published: Date(1859, 11, 1)
                )
        ];

        foreach(ref b; books)
            db.create(b);

        // create reviews
        auto reviews = [
            review(
                    book_id: books[0].id,
                    comment: "Fantastic book! This is the language I always dreamed of!",
                    rating: 5,
                    recommended: MyBool(true)
                  ),
            review(
                    book_id: books[0].id,
                    comment: "Who wrote this garbage? We all know that crabby ownership tracking is the future!",
                    rating: 1,
                    recommended: MyBool(false)
                  ),
            review(
                    book_id: books[1].id,
                    comment: "Before this book, I was completely lost with templates. Now I am still completely lost with templates, but at least I know that there are cool things you can do!",
                    rating: 4,
                    recommended: MyBool(true)
                  ),
            review(
                    book_id: books[4].id,
                    comment: "I had to read this for English lit, but it's a very confusing story. Why is there so much 'Death by segfault'? What even is a segfault?",
                    rating: 2,
                    recommended: MyBool(false)
                  )
        ];

        foreach(ref rev; reviews)
            db.create(rev);

        return 0;
    }

    auto db = Database(datafilename);

    args = args[1 .. $];

    // function to match commands
    bool matchCommand(string[] commands...)
    {
        if(args.length < commands.length)
            return false;
        import std.uni;
        import std.algorithm.comparison;
        foreach(i; 0 .. commands.length)
        {
            if(!equal(commands[i], args[i].asLowerCase))
                return false;
        }
        args = args[commands.length .. $];
        return true;
    }
    DataSet!book bds;

    // else, we are going to run a query
    // command: books by <name>
    if(matchCommand("books", "by"))
    {
        // fetch all books ordered by title, which match the given author name.
        auto query = select(bds)
                    .where("(", bds.author.firstName, ` LIKE "%" || `, args[0].param, ` || "%") OR (`,
                        bds.author.lastName, ` LIKE "%" || `, args[0].param, ` || "%")`)
                    .orderBy(bds.title);
        writeln(db.fetch(query));
    }
    else if(matchCommand("books", "named"))
    {
        // fetch all books ordered by title, which match the given title.
        auto query = select(bds)
                    .where(bds.title, ` LIKE "%" || `, args[0].param, ` || "%"`)
                    .orderBy(bds.title);
        writeln(db.fetch(query));
    }
    else if(matchCommand("books"))
    {
        // just list all the books.
        auto query = select(bds)
            .orderBy(bds.title);
        writeln(db.fetch(query));
    }
    else
    {
        writeln("unknown command: ", args);
        return 1;
    }
    return 0;
}
