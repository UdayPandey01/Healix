export const users = [
  { id: 1, name: "Ada", email: "ada@example.com" },
  { id: 2, name: "Grace", email: "grace@example.com" },
  { id: 3, name: "Alan", email: "alan@example.com" },
];

export const orders = [
  { id: 101, userId: 1, item: "Keyboard", total: 49.99 },
  { id: 102, userId: 1, item: "Monitor", total: 199.0 },
  { id: 103, userId: 2, item: "Mouse", total: 24.5 },
  { id: 104, userId: 2, item: "Desk", total: 320.0 },
  { id: 105, userId: 3, item: "Lamp", total: 35.0 },
  { id: 106, userId: 3, item: "Chair", total: 210.0 },
  { id: 107, userId: 1, item: "Cable", total: 9.99 },
];

export function findUser(id) {
  return users.find((u) => u.id === id);
}
