"""Label runs with the business records they touched.

.. code-block:: python

    from flowstash.records import record_scope, link_records

    with record_scope(upc, record_type="upc"):
        await push(upc)          # logs here carry fw.record_key

    link_records(ok_upcs, record_type="upc")

See :mod:`flowstash.observability.record_links` for details.
"""

from .observability.record_links import link_record, link_records, record_scope

__all__ = ["record_scope", "link_record", "link_records"]
