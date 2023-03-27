from neo4j import GraphDatabase


class Neo4J:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def save_uniprot_entry(self, entry, protein_id):
        with self.driver.session(database="neo4j") as session:
            # create Protein
            protein = self.add_protein(entry, protein_id, session)

            # create protein FullName
            protein_full_name = self.add_protein_full_name(protein, session)

            # create relationship between Protein and FullName
            session.execute_write(self.connect_protein_to_full_name,
                                  protein_id=protein_id,
                                  full_name=protein_full_name)

            # create organism
            organism_taxonomy_id = self.add_organism(entry, session)

            # create relationship between Protein and Organism
            session.execute_write(self.connect_protein_to_organism,
                                  protein_id=protein_id,
                                  organism_id=organism_taxonomy_id)

            # create genes
            self.add_genes(entry, session)

            # connect protein to genes
            session.execute_write(self.connect_protein_to_genes, protein_id=protein_id)

            print('Done')

    def add_genes(self, entry, session):
        genes = entry.find('{http://uniprot.org/uniprot}gene')
        for gene in genes.findall('{http://uniprot.org/uniprot}name'):
            gene_type = gene.attrib['type']
            gene_name = gene.text
            session.execute_write(self.create_gene,
                                  gene_type=gene_type,
                                  gene_name=gene_name)

    def add_organism(self, entry, session):
        organism = entry.find('{http://uniprot.org/uniprot}organism')
        organism_scientific_name = organism.find("{http://uniprot.org/uniprot}name[@type='scientific']").text
        organism_common_name = organism.find("{http://uniprot.org/uniprot}name[@type='common']").text
        organism_taxonomy_id = organism.find("{http://uniprot.org/uniprot}dbReference").attrib['id']
        session.execute_write(self.create_organism,
                              scientific_name=organism_scientific_name,
                              common_name=organism_common_name,
                              taxonomy_id=organism_taxonomy_id)
        return organism_taxonomy_id

    def add_protein_full_name(self, protein, session):
        recommended_name = protein.find('{http://uniprot.org/uniprot}recommendedName')
        protein_full_name = recommended_name.find('{http://uniprot.org/uniprot}fullName').text
        session.execute_write(self.create_protein_full_name,
                              protein_full_name=protein_full_name)
        return protein_full_name

    def add_protein(self, entry, protein_id, session):
        protein = entry.find('{http://uniprot.org/uniprot}protein')
        session.execute_write(self.create_protein, protein_id=protein_id)
        return protein

    @staticmethod
    def connect_protein_to_genes(tx, protein_id):
        q = """
            MATCH (p:Protein),(g:Gene)
            WHERE p.id = $protein_id
            CREATE (p)-[r:FROM_GENE {status:g.type}]->(g)
            RETURN type(r)
            """
        res = tx.run(q, protein_id=protein_id)
        return res

    @staticmethod
    def create_gene(tx, gene_type, gene_name):
        q = """
                CREATE (g:Gene {name: $gene_name,
                                type: $gene_type})
                RETURN g.name as name
                """
        res = tx.run(q, gene_name=gene_name,
                     gene_type=gene_type)
        return [i for i in res][0]['name']

    @staticmethod
    def connect_protein_to_organism(tx, protein_id, organism_id):
        q = """
            MATCH (p:Protein),(o:Organism)
            WHERE p.id = $protein_id AND o.taxonomy_id = $taxonomy_id
            CREATE (p)-[r:IN_ORGANISM]->(o)
            RETURN type(r)
            """
        res = tx.run(q, protein_id=protein_id, taxonomy_id=organism_id)
        return res

    @staticmethod
    def create_organism(tx, scientific_name, common_name, taxonomy_id):
        q = """
            CREATE (o:Organism {scientific_name: $scientific_name,
                                common_name: $common_name,
                                taxonomy_id: $taxonomy_id})
            RETURN o.taxonomy_id as taxonomy_id
            """
        res = tx.run(q, scientific_name=scientific_name,
                     common_name=common_name,
                     taxonomy_id=taxonomy_id)
        return [i for i in res][0]['taxonomy_id']

    @staticmethod
    def connect_protein_to_full_name(tx, protein_id, full_name):
        q = """
        MATCH (p:Protein),(f:FullName)
        WHERE p.id = $id AND f.name = $name
        CREATE (p)-[r:HAS_FULL_NAME]->(f)
        RETURN type(r)
        """
        res = tx.run(q, id=protein_id, name=full_name)
        return res

    @staticmethod
    def create_protein_full_name(tx, protein_full_name) -> str:
        q = """
        CREATE (pn:FullName {name: $name})
        RETURN pn.name as name
        """
        res = tx.run(q, name=protein_full_name)
        return [i for i in res][0]['name']

    @staticmethod
    def create_protein(tx, protein_id) -> str:
        q = """
        CREATE (p:Protein {id: $id})
        RETURN p.id as id
        """
        res = tx.run(q, id=protein_id)
        return [i for i in res][0]['id']
