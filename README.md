# PSS Fleet Data Importer

<a href="https://discord.gg/kKguSec" target="_blank"><img src="https://discord.com/api/guilds/565819215731228672/embed.png" alt="Support Discord server invite"></a>
<a href="https://codecov.io/gh/Zukunftsmusik/pss-fleet-data-api"><img src="https://codecov.io/gh/Zukunftsmusik/pss-fleet-data-api/graph/badge.svg?token=M7GZSCGK36"/></a>

> A tool to import [PSS Fleet Data](https://github.com/Zukunftsmusik/pss-fleet-data) from google drive to the [PSS Fleet Data API](https://github.com/Zukunftsmusik/pss-fleet-data-api).

## Built with

- [Docker](https://www.docker.com/)
- [PostgreSQL](https://www.postgresql.org/)
- [Pydantic V2](https://docs.pydantic.dev/latest/)
- [PyDrive2](https://docs.iterative.ai/PyDrive2/)
- [SQLModel](https://sqlmodel.tiangolo.com/)

# 🚀 Deploy
Upon starting the Importer will create the database and the required tables.
In order to deploy the Importer, the following prerequisites must be met:

- A [PostgreSQL 14](https://www.postgresql.org/) server running.
- Optional: a current [Docker](https://www.docker.com/) installation when deploying locally with Docker.
- A [Google Service Account](https://support.google.com/a/answer/7378726?hl=en). After adding a key to the Google Service Account, download the `client_secrets*.json` and store it in a safe place.
- A [client_secrets.json](https://docs.iterative.ai/PyDrive2/quickstart/#authentication) in the workspace folder containing your service account credentials. Alternatively you can pass the information required for this file via environment variables (see below). **NEVER DEPLOY THIS FILE ANYWHERE PUBLICLY!**
- Certain environment variables set:

## Required Environment variables
- `DATABASE_URL`: The URL to the database server, including username, password, server IP or name and port.

## Optional environment variables
- `DATABASE_ENGINE_ECHO`: Set to `true` to have SQL statements printed to stdout.
- `DATABASE_NAME`: The name of the database. Will be overriden during tests. Defaults to `pss-fleet-data-importer`.
- `DEBUG_MODE`: Set to `true` to start the application in debug mode. Enables more verbose logging.
- `FLEET_DATA_API_KEY`: Your API key that might be required to access `DELETE` and `POST` endpoints. Whether such an API key is required depends on the [PSS Fleet Data API](https://github.com/Zukunftsmusik/pss-fleet-data-api) instance you want to use.
- `FLEET_DATA_API_URL`: Sets the base URL of the **PSS Fleet Data API** server to use. Defaults to `https://fleetdata.dolores2.xyz`.
- `GDRIVE_SERVICE_PROJECT_ID`: The name of the project your **Google Service Account** is tied to. E.g. `project-name`.
- `GDRIVE_PRIVATE_KEY`: The private key of the **Google Service Account**. <sup>1</sup>
- `GDRIVE_PRIVATE_KEY_ID`: The ID of the private key of the **Google Service Account**.
- `GDRIVE_CLIENT_EMAIL`: The e-mail address of the **Google Service Account**, e.g. `abc@project-name.iam.gserviceaccount.com`.
- `GDRIVE_CLIENT_ID`: The OAuth 2 Client ID of the **Google Service Account**.
- `GDRIVE_FOLDER_ID`: The ID of the Google Drive folder with the collected [PSS Fleet Data](https://github.com/Zukunftsmusik/pss-fleet-data). Defaults to `10wOZgAQk_0St2Y_jC3UW497LVpBNxWmP`.
- `KEEP_DOWNLOADED_FILES`: Set tp `true` to keep Collections downloaded from the Google Drive folder on disk after importing them.
- `REINITIALIZE_DATABASE`: Set to `true` to drop all tables at app start before recreating them.

> <sup>1</sup> = When using this environment variable, the value needs to follow a certain format, since it's a multiline text:
> ```
> export GDRIVE_SERVICE_PRIVATE_KEY="$(cat <<EOF
> -----BEGIN PRIVATE KEY-----
> {25 lines of 64 characters each}
> {1 line of 24 characters}
> -----END PRIVATE KEY-----
> EOF
> )"
> ```

## Deploy on CapRover
To deploy the Importer on [CapRover](https://caprover.com/) you need to:
- Create a new app on your CapRover instance.
- Set up the environment variables described above.
- [Fork the repository](https://github.com/Zukunftsmusik/pss-fleet-data-importer/fork) to your own github account.
- Follow the steps explained in the [CapRover Docs](https://caprover.com/docs/ci-cd-integration/deploy-from-github.html).

## Deploy locally with Docker
Follow the steps outlined in the [Contribution Guide](CONTRIBUTING.md) to set up your local development environment. Take the `client_secrets*.json` file downlaoded after adding a key to the **Google Service Account**, rename it it `client_secrets.json` and place it in the workspace folder. Create a file called `.docker-env` in the workspace folder and add the following environment variables:
- `DATABASE_URL`
- `FLEET_DATA_API_URL`
- `DATABASE_NAME` (optional)
- `FLEET_DATA_API_KEY` (optional)

Then open a terminal, navigate to the workspace folder and run `make docker`. The command will:
- Stop the running container, if it's been started with the same command.
- Delete the stopped container
- Delete the image, if it's been created with the same command.
- Build a new image.
- Start a container with that image.

## Run locally without Docker
- Follow the steps outlined in the [Contribution Guide](CONTRIBUTING.md) to set up your local development environment.
- Set up the environment variables outlined above.
- Open a terminal, navigate to the workspace folder and run `make run` to start the Importer.

# 🖊️ Contribute
If you ran across a bug or have a feature request, please check if there's [already an issue](https://github.com/Zukunftsmusik/pss-fleet-data-importer/issues) for that and if not, please [open a new one](https://github.com/Zukunftsmusik/pss-fleet-data-importer/issues/new).

If you want to fix a bug or add a feature, please check out the [Contribution Guide](CONTRIBUTING.md).

# 🆘 Support
If you need help using the Importer or want to contribute, you can join my support Discord at: [discord.gg/kKguSec](https://https://discord.gg/kKguSec)

# 🔗 Links
- Documentation (tbd)
- [Official Support Discord server](https://https://discord.gg/kKguSec)
- [Official PSS Fleet Data API](https://fleetdata.dolores2.xyz)
- [Buy me a can of cat food](https://buymeacoffee.com/the_worst_pss)
- [Or a coffee](https://ko-fi.com/theworstpss)
