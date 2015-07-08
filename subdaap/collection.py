from daapserver import collection

from subdaap import utils


class MutableCollection(collection.MutableCollection):

    def __init__(self, *args, **kwargs):
        super(MutableCollection, self).__init__(*args, **kwargs)

        self.ready = self.revision != -1
        self.busy = False
        self.iter_item = None

    def count(self):
        """
        """

        # Prepare query depending on `self.child_class`. Use name to prevent
        # cyclic imports.
        child_class_name = self.child_class.__name__

        if child_class_name == "Database":
            query = """
                SELECT
                    COUNT(*)
                FROM
                    `databases`
                WHERE
                    `databases`.`exclude` = 0
                LIMIT 1
                """,
        elif child_class_name == "Item":
            query = """
                SELECT
                    COUNT(*)
                FROM
                    `items`
                LEFT OUTER JOIN
                    `artists` ON `items`.`artist_id`=`artists`.`id`
                LEFT OUTER JOIN
                    `albums` ON `items`.`album_id`=`albums`.`id`
                WHERE
                    `items`.`database_id` = ? AND
                    `items`.`exclude` = 0 AND
                    COALESCE(`artists`.`exclude`, 0) = 0 AND
                    COALESCE(`albums`.`exclude`, 0) = 0
                LIMIT 1
                """, self.parent.id
        elif child_class_name == "Container":
            query = """
                SELECT
                    COUNT(*)
                FROM
                    `containers`
                WHERE
                    `containers`.`database_id` = ? AND
                    `containers`.`exclude` = 0
                LIMIT 1
                """, self.parent.id
        elif child_class_name == "ContainerItem":
            query = """
                SELECT
                    COUNT(*)
                FROM
                    `container_items`
                INNER JOIN
                    `items` ON `container_items`.`id`=`items`.`id`
                LEFT OUTER JOIN
                    `artists` ON `items`.`artist_id`=`artists`.`id`
                LEFT OUTER JOIN
                    `albums` ON `items`.`album_id`=`albums`.`id`
                WHERE
                    `container_items`.`database_id` = ? AND
                    `container_items`.`container_id` = ? AND
                    COALESCE(`items`.`exclude`, 0) = 0 AND
                    COALESCE(`artists`.`exclude`, 0) = 0 AND
                    COALESCE(`albums`.`exclude`, 0) = 0
                LIMIT 1
                """, self.parent.id, self.parent.database_id

        # Execute query.
        with self.parent.db.get_cursor() as cursor:
            return cursor.query_value(*query)

    def load(self, item_ids=None):
        """
        """

        # Only one invocation at a time.
        if self.busy:
            raise ValueError("Already busy loading items.")

        # Prepare query depending on `self.child_class`. Use name to prevent
        # cyclic imports.
        child_class_name = self.child_class.__name__

        if item_ids:
            if child_class_name == "Database":
                in_clause = " AND `databases`.`id` IN (%s)"
            elif child_class_name == "Item":
                in_clause = " AND `items`.`id` IN (%s)"
            elif child_class_name == "Container":
                in_clause = " AND `containers`.`id` IN (%s)"
            elif child_class_name == "ContainerItem":
                in_clause = " AND `container_items`.`id` IN (%s)"

            in_clause = in_clause % utils.in_list(item_ids)
        else:
            in_clause = ""

        if child_class_name == "Database":
            query = """
                SELECT
                    `databases`.`id`,
                    `databases`.`persistent_id`,
                    `databases`.`name`
                FROM
                    `databases`
                WHERE
                    `databases`.`exclude` = 0
                    %s
                """ % in_clause,
        elif child_class_name == "Item":
            query = """
                SELECT
                    `items`.`id`,
                    `items`.`database_id`,
                    `items`.`persistent_id`,
                    `items`.`remote_id`,
                    `items`.`name`,
                    `items`.`track`,
                    `items`.`year`,
                    `items`.`bitrate`,
                    `items`.`duration`,
                    `items`.`file_size`,
                    `items`.`file_name`,
                    `items`.`file_type`,
                    `items`.`file_suffix`,
                    `items`.`genre`,
                    `artists`.`name` as `artist`,
                    `albums`.`name` as `album`,
                    `albums`.`art` as `album_art`
                FROM
                    `items`
                LEFT OUTER JOIN
                    `artists` ON `items`.`artist_id` = `artists`.`id`
                LEFT OUTER JOIN
                    `albums` ON `items`.`album_id` = `albums`.`id`
                WHERE
                    `items`.`database_id` = ? AND
                    `items`.`exclude` = 0 AND
                    COALESCE(`artists`.`exclude`, 0) = 0 AND
                    COALESCE(`albums`.`exclude`, 0) = 0
                    %s
                """ % in_clause, self.parent.id
        elif child_class_name == "Container":
            query = """
                SELECT
                    `containers`.`id`,
                    `containers`.`database_id`,
                    `containers`.`persistent_id`,
                    `containers`.`parent_id`,
                    `containers`.`name`,
                    `containers`.`is_base`,
                    `containers`.`is_smart`
                FROM
                    `containers`
                WHERE
                    `containers`.`database_id` = ? AND
                    `containers`.`exclude` = 0
                    %s
                """ % in_clause, self.parent.id
        elif child_class_name == "ContainerItem":
            query = """
                SELECT
                    `container_items`.`id`,
                    `container_items`.`item_id`,
                    `container_items`.`container_id`
                FROM
                    `container_items`
                INNER JOIN
                    `items` ON `container_items`.`id` = `items`.`id`
                LEFT OUTER JOIN
                    `artists` ON `items`.`artist_id` = `artists`.`id`
                LEFT OUTER JOIN
                    `albums` ON `items`.`album_id` = `albums`.`id`
                WHERE
                    `container_items`.`container_id` = ? AND
                    COALESCE(`items`.`exclude`, 0) = 0 AND
                    COALESCE(`artists`.`exclude`, 0) = 0 AND
                    COALESCE(`albums`.`exclude`, 0) = 0
                    %s
                """ % in_clause, self.parent.id

        # Execute query.
        try:
            self.busy = True

            # Convert rows to items. Iterate over chunck for cache
            # improvements.
            store = self.store
            child_class = self.child_class
            db = self.parent.db

            with self.parent.db.get_cursor() as cursor:
                for rows in utils.chunks(cursor.query(*query), 25):
                    for row in rows:
                        # Create new instance from a row. Note that since the
                        # instance is slotted, the row keys should match!
                        item = child_class(db, **row)

                        # Add to store
                        store.add(item.id, item)

                        # Yield result
                        self.iter_item = item
                        yield item

                # Mark as done
                if not item_ids:
                    self.ready = True
        finally:
            self.busy = False

    def update_ids(self, item_ids):
        """
        """

        # Don't update if this instance isn't ready.
        if not self.ready:
            return

        utils.exhaust(self.load(item_ids))

    def remove_ids(self, item_ids):
        """
        """

        # Don't remove items if this instance isn't ready.
        if not self.ready:
            return

        for item_id in item_ids:
            self.store.remove(item_id)

    def commit(self, revision):
        """
        """

        super(MutableCollection, self).commit(revision)
        self.ready = False

    def __contains__(self, key):
        """
        """

        if not self.ready:
            utils.exhaust(self.load())

        return super(MutableCollection, self).__contains__(key)

    def __len__(self):
        """
        """

        if not self.ready:
            return self.count()

        return super(MutableCollection, self).__len__()

    def __getitem__(self, key):
        """
        """

        if self.busy and self.iter_item.id == key:
            return self.iter_item

        if not self.ready:
            utils.exhaust(self.load())

        return super(MutableCollection, self).__getitem__(key)

    def iterkeys(self):
        """
        """

        if not self.ready:
            for item in self.load():
                yield item.id
        else:
            for key in super(MutableCollection, self).iterkeys():
                yield key

    def itervalues(self):
        """
        """

        if not self.ready:
            for item in self.load():
                yield item
        else:
            for item in super(MutableCollection, self).itervalues():
                yield item
