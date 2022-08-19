"""Test the validity of all DAGs."""

from airflow.models import DagBag


def test_dagbag():
    """
    Validate DAG files using Airflow's DagBag.
    This includes sanity checks e.g. do tasks have required arguments, are DAG ids unique & do DAGs have no cycles.
    """
    dag_bag = DagBag(include_examples=False)
    assert (
        not dag_bag.import_errors
    )  # Import errors aren't raised but captured to ensure all DAGs are parsed
