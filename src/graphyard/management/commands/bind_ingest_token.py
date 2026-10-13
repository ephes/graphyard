from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError

from graphyard.influx import normalize_subject_id
from graphyard.models import IngestToken
from graphyard.token_scope import clean_host_scope


class Command(BaseCommand):
    help = "Bind an ingest token to one subject/host, by token name (never prints credentials)."

    def add_arguments(self, parser):
        parser.add_argument("--name", required=True)
        parser.add_argument(
            "--subject",
            required=True,
            help="Exact subject/host identifier; no wildcards",
        )

    def handle(self, *args, **options):
        try:
            subject = clean_host_scope([options["subject"]])[0]
            if "*" in subject:
                raise ValueError("Binding must be an exact subject/host identifier")
            binding = normalize_subject_id(subject)
        except (ValidationError, ValueError) as err:
            raise CommandError(str(err)) from err
        if not IngestToken.objects.filter(name=options["name"]).update(
            bound_subject_id=binding
        ):
            raise CommandError("No ingest token with that name")
        self.stdout.write(f"name={options['name']} binding={binding}")
