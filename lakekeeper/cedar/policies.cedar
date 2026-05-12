// Lakekeeper Cedar Policies
//
// Mapped to Keycloak groups via LAKEKEEPER__OPENID_ROLES_CLAIM=groups.
// Lakekeeper-Rollen-Namen müssen exakt den Keycloak-Gruppen-Namen entsprechen.
//
// Action-/Resource-Namen sind Initialskizze — nach dem Cedar-Switch via
// GET $LAKEKEEPER/cedar-schema verifizieren und ggf. anpassen.

// IT-Admin: Vollzugriff (Keycloak-Gruppe "IT-Admin")
permit (
  principal in Lakekeeper::Role::"IT-Admin",
  action,
  resource
);

// Viewer: nur lesender Zugriff auf Warehouses (Keycloak-Default-Gruppe "Viewer")
permit (
  principal in Lakekeeper::Role::"Viewer",
  action == Lakekeeper::Action::"read",
  resource is Lakekeeper::Warehouse
);
