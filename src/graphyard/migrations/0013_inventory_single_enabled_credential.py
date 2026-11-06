from django.db import migrations, models
from django.db.models import Count


def reject_duplicates(apps, schema_editor):
    credentials = apps.get_model("graphyard", "InventoryCredential")
    duplicates = list(
        credentials.objects.using(schema_editor.connection.alias)
        .filter(enabled=True)
        .values("host_id")
        .annotate(total=Count("pk"))
        .filter(total__gt=1)
        .order_by("host_id")
        .values_list("host_id", flat=True)
    )
    if duplicates:
        raise RuntimeError(
            "multiple enabled inventory credentials for inventory host IDs: "
            + ", ".join(map(str, duplicates))
            + "; revoke duplicates explicitly before migrating"
        )


class Migration(migrations.Migration):
    dependencies = [("graphyard", "0012_ingest_token_subject_binding")]
    operations = [
        migrations.RunPython(reject_duplicates, migrations.RunPython.noop),
        migrations.AddConstraint(
            model_name="inventorycredential",
            constraint=models.UniqueConstraint(
                fields=["host"],
                condition=models.Q(enabled=True),
                name="inventory_one_enabled_credential",
            ),
        ),
    ]
