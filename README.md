# PeopleDB
PeopleDB is a Java-based project that demonstrates a simple CRUD (Create, Read, Update, Delete) application. This project is developed using Test-Driven Development (TDD) methodologies. It interfaces with an SQL database to manage data about people, including their addresses (home and business), spouse, children, and other personal details.

# Features
CRUD operations for Person entities. Management of related data such as home and business addresses. Handling relationships like spouse and children. TDD approach ensuring robust and reliable code.

# Technology Stack
Java SQL (Using H2 Database) JUnit for testing

# Installation and Setup
Clone the Repository: Clone this repository to your local machine using git clone.

Database Setup: Ensure you have an SQL database running. This project uses an H2 database, but you can configure it to use another SQL database.

Configure Database Connection: Update the database connection details in PeopleRepositoryTest.java to point to your database.

Build the Project: Compile the project using your preferred Java build tool (e.g., Maven, Gradle).

# Usage
After setting up the project, you can perform various operations related to people management. Examples include:

Saving a Person: Create a new Person object and use the save method in PeopleRepository to persist it to the database. Finding a Person by ID: Use the findById method in PeopleRepository to retrieve a person's details. Updating a Person: Modify a Person object and use the update method to apply changes. Deleting a Person: Use the delete method to remove a person from the database. Running Tests To run the tests, use the command:

mvn test or run tests directly from your IDE.

# Contributing 
Contributions to this project are welcome. Please adhere to the following steps:

Fork the repository. Create a new branch for your feature. Commit your changes. Push to the branch. Open a pull request.
