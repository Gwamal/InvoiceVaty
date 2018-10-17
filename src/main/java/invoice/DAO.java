package invoice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class DAO {

	private final DataSource myDataSource;

	/**
	 *
	 * @param dataSource la source de données à utiliser
	 */
	public DAO(DataSource dataSource) {
		this.myDataSource = dataSource;
	}

	/**
	 * Renvoie le chiffre d'affaire d'un client (somme du montant de ses factures)
	 *
	 * @param id la clé du client à chercher
	 * @return le chiffre d'affaire de ce client ou 0 si pas trouvé
	 * @throws SQLException
	 */
	public float totalForCustomer(int id) throws SQLException {
		String sql = "SELECT SUM(Total) AS Amount FROM Invoice WHERE CustomerID = ?";
		float result = 0;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id); // On fixe le 1° paramètre de la requête
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getFloat("Amount");
				}
			}
		}
		return result;
	}

	/**
	 * Renvoie le nom d'un client à partir de son ID
	 *
	 * @param id la clé du client à chercher
	 * @return le nom du client (LastName) ou null si pas trouvé
	 * @throws SQLException
	 */
	public String nameOfCustomer(int id) throws SQLException {
		String sql = "SELECT LastName FROM Customer WHERE ID = ?";
		String result = null;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id);
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getString("LastName");
				}
			}
		}
		return result;
	}

	/**
	 * Transaction permettant de créer une facture pour un client
	 *
	 * @param customer Le client
	 * @param productIDs tableau des numéros de produits à créer dans la facture
	 * @param quantities tableau des quantités de produits à facturer faux sinon Les deux tableaux doivent avoir la même
	 * taille
	 * @throws java.lang.Exception si la transaction a échoué
	 */
	public void createInvoice(CustomerEntity customer, int[] productIDs, int[] quantities)
                    throws Exception {
                // Requete sql a parametre pour la création d'une facture => table INVOICE
                String sqlIstInvoice = "INSERT INTO INVOICE (CustomerID) VALUES (?)";
                // Requete sql pour modifier la table invoice pour ajouter le prix de la commande
                String sqlUpdInvoice = "UPDATE INVOICE SET CustomerID=? WHERE ID=?";
                // Requete sql a parametre pour l'ajout dans la liste d'item de la facture => table ITEM
                String sqlIstItems = "INSERT INTO ITEM (InvoiceID, Item, ProductID, Quantity, Cost) VALUES(?,?,?,?,?)";
                // On récupere le prix d'un produit
                String sqlPriceProdu = "SELECT Price FROM PRODUCT WHERE ID = ?";

                // Compteur pour le nombre de ligne a ajouter
                int cpt = 0;
                // Clé primaire de invoice
                int invoiceKey = 0;
                // Cout unitaire d'un produit
                float priceProductUnity = 0;
                
                int quantitiesProduct = 0;
                
                // EXCEPTION
                String mess1 = "Produit Inconnu";
                String mess2 = "Quantite Incorrect";
                String mess3 = "Tableau taille différente";
                
                //Connection a la bd => utilisation du try with ressources
                try(Connection connection = myDataSource.getConnection();
                      PreparedStatement stmIstInvoice = connection.prepareStatement(sqlIstInvoice);
                      PreparedStatement stmIstItems = connection.prepareStatement(sqlIstItems);
                      PreparedStatement stmUpdInvoice = connection.prepareStatement(sqlUpdInvoice);
                      PreparedStatement stmPriceProduct = connection.prepareStatement(sqlPriceProdu)){
                      
                    // On verifie si le tableau de productsID a la meme taille que le tableau quantite
                    if (productIDs.length==quantities.length){
                        // Si un produit est introuvable dans la table on lance une erreur
                        for (int i = 0; i < productIDs.length; i ++){
                            if (!findProduct(productIDs[i])){
                                throw new Exception(mess1);
                            }   
                        }
                        // Si une quantite de produit est négatif ou equal a zero on lance une erreur
                        for (int i = 0; i < quantities.length; i++){
                            if (quantities[i] <= 0) {
                                throw new Exception(mess2);
                            }   
                        }
                        
                        // Toute les valeur sont correcte dans les deux tableaux
                        // On récupere l'id du client
                        int customerKey = customer.getCustomerId();
                        // on crée une nouvelle facture (invoice) sans la prix total
                        stmIstInvoice.setInt(1,customerKey);
                        stmIstInvoice.executeUpdate();
                        
                        // On récupere la derniere clé autogénérer de InvoiceID
                        ResultSet generatedInvoiceKey = stmIstInvoice.getGeneratedKeys();
                        while(generatedInvoiceKey.next()){
                            invoiceKey = generatedInvoiceKey.getInt(1);
                        }
                        
                        //Pour chaque produits on créer une ligne lié a la facture
                        for (;cpt < productIDs.length;cpt++){
                            int productId = productIDs[cpt];
                            quantitiesProduct = quantities[cpt];
                            
                            // On récuperer le prix du produit
                            stmPriceProduct.setInt(1, productId);
                            ResultSet rsPrd = stmPriceProduct.executeQuery();
                            if (rsPrd.next()){
                                priceProductUnity = rsPrd.getFloat("Price");
                            }
                           
                            // Une des clé primaire de Item
                            stmIstItems.setInt(1, invoiceKey);
                            stmIstItems.setInt(2, cpt);
                            stmIstItems.setInt(3, productId); 
                            stmIstItems.setInt(4,quantitiesProduct);
                            //stmIstItems.setFloat(5,((priceProductUnity)*quantitiesProduct)); ==> N UTILISE PAS LE TRIGGER
                            stmIstItems.setFloat(5,priceProductUnity);
                            // On execute la requete
                            stmIstItems.executeUpdate();
                        }                      
                        // On récuperer les valeur pour les mettre dans l'update
                        // ==> CODE A CHANGER SI ON SE SERT PAS DU TRIGGER
                        //On remet a jour la facture du client avec son id pour utiliser le trigger du total
                        stmUpdInvoice.setInt(1, customerKey);
                        stmUpdInvoice.setInt(2, invoiceKey);
                        stmUpdInvoice.executeUpdate();
                                       
                    } else {
                       throw new Exception(mess3);
                    }                  
                }catch(Exception ex){
                    throw new Exception(ex.getMessage());
                }
                
        }
        
	/**
	 *
	 * @return le nombre d'enregistrements dans la table CUSTOMER
	 * @throws SQLException
	 */
	public int numberOfCustomers() throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Customer";
		try (Connection connection = myDataSource.getConnection();
			Statement stmt = connection.createStatement()) {
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 *
	 * @param customerId la clé du client à recherche
	 * @return le nombre de bons de commande pour ce client (table PURCHASE_ORDER)
	 * @throws SQLException
	 */
	public int numberOfInvoicesForCustomer(int customerId) throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Invoice WHERE CustomerID = ?";

		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerId);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

        /**
	 * Trouver un produit à partir de sa clé
	 *
	 * @param ID la clé du PRODUCT à rechercher
	 * @return un boolean si on trouve un produit correspondant a la clé
	 * @throws SQLException
	 */
	boolean findProduct(int ID) throws SQLException {
		boolean result = false;

		String sql = "SELECT * FROM Product WHERE ID = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, ID);

			ResultSet rs = stmt.executeQuery();
                        // Si on trouve au moins une ligne correspondant au produit on renvoie vrai
			if (rs.next()) {
				result = true;
			}
		}
		return result;
	}
        
	/**
	 * Trouver un Customer à partir de sa clé
	 *
	 * @param customedID la clé du CUSTOMER à rechercher
	 * @return l'enregistrement correspondant dans la table CUSTOMER, ou null si pas trouvé
	 * @throws SQLException
	 */
	CustomerEntity findCustomer(int customerID) throws SQLException {
		CustomerEntity result = null;

		String sql = "SELECT * FROM Customer WHERE ID = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerID);

			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				String name = rs.getString("FirstName");
				String address = rs.getString("Street");
				result = new CustomerEntity(customerID, name, address);
			}
		}
		return result;
	}

	/**
	 * Liste des clients localisés dans un état des USA
	 *
	 * @param state l'état à rechercher (2 caractères)
	 * @return la liste des clients habitant dans cet état
	 * @throws SQLException
	 */
	List<CustomerEntity> customersInCity(String city) throws SQLException {
		List<CustomerEntity> result = new LinkedList<>();

		String sql = "SELECT * FROM Customer WHERE City = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, city);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					int id = rs.getInt("ID");
					String name = rs.getString("FirstName");
					String address = rs.getString("Street");
					CustomerEntity c = new CustomerEntity(id, name, address);
					result.add(c);
				}
			}
		}

		return result;
	}
}
