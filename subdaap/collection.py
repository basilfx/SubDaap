from daapserver import collection

from subdaap import utils


class LazyMutableCollection(collection.LazyMutableCollection):

    __slots__ = collection.LazyMutableCollection.__slots__ + ("child_class", )

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
                    `items` ON `container_items`.`item_id`=`items`.`id`
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
                    `items` ON `container_items`.`item_id` = `items`.`id`
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
                        # Update an existing item
                        if item_ids:
                            try:
                                item = store.get(row["id"])

                                for key in row.keys():
                                    setattr(item, key, row[key])
                            except KeyError:
                                item = child_class(db, **row)
                        else:
                            item = child_class(db, **row)

                        # Add to store
                        store.add(item.id, item)

                        # Yield result
                        self.iter_item = item
                        yield item

            # Final actions after all items have been loaded
            if not item_ids:
                self.ready = True

                if self.pending_commit != -1:
                    revision = self.pending_commit
                    self.pending_commit = -1
                    self.commit(revision)
        finally:
            self.busy = False
