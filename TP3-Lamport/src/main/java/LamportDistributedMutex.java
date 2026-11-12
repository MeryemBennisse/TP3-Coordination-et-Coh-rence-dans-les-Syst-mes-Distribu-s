import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LamportDistributedMutex {

    // Nombre total de processus simulés
    private static final int NB_PROCESSUS = 3;

    private static final Comparator<Message> ORDRE_LAMPORT =
            Comparator.comparingInt((Message m) -> m.horloge).thenComparingInt(m -> m.idSource);

    // Canal réseau simulé (messages envoyés avec délai)
    static class CanalReseau {
        private final Random random = new Random();
        private final AtomicInteger compteurMessages = new AtomicInteger(0);
        private final Map<String, ExecutorService> liens = new ConcurrentHashMap<>();

        // Envoi d’un message avec délai simulé
        public void envoyerMessage(Processus src, Processus dest, Message msg) {
            compteurMessages.incrementAndGet(); // Incrémenter le compteur global
            int delai = 300 + random.nextInt(500); // Délai aléatoire entre 300 et 800 ms
            liens.computeIfAbsent(src.id + "->" + dest.id, k -> Executors.newSingleThreadExecutor()).submit(() -> {
                try {
                    Thread.sleep(delai); // Simule la latence du réseau
                    dest.recevoirMessage(msg);
                } catch (InterruptedException ignored) {
                }
            });
        }

        public int getNombreMessages() {
            return compteurMessages.get();
        }
    }

    // Types de message possibles
    enum TypeMessage {
        REQUEST, REPLY, RELEASE
    }

    // Classe représentant un message Lamport
    static class Message {
        TypeMessage type;
        int horloge;
        int idSource;

        public Message(TypeMessage type, int horloge, int idSource) {
            this.type = type;
            this.horloge = horloge;
            this.idSource = idSource;
        }
    }

    // Classe représentant un processus dans le système
    static class Processus implements Runnable {
        int id;
        int horlogeLamport = 0;
        CanalReseau canal;
        List<Processus> autres;
        volatile boolean enPanne = false;
        Queue<Message> fileRequetes = new PriorityBlockingQueue<>(11, ORDRE_LAMPORT);
        Set<Integer> reponsesRecues = ConcurrentHashMap.newKeySet();

        public Processus(int id, CanalReseau canal) {
            this.id = id;
            this.canal = canal;
        }

        // Fonction pour mettre un processus en panne
        public void setEnPanne(boolean enPanne) {
            this.enPanne = enPanne;
        }

        // Mise à jour de l'horloge de Lamport
        private synchronized void incrementerHorloge() {
            horlogeLamport++;
        }

        private synchronized void majHorloge(int autreHorloge) {
            horlogeLamport = Math.max(horlogeLamport, autreHorloge) + 1;
        }

        // Envoi d’un message à tous les autres processus
        public synchronized Message diffuserMessage(TypeMessage type) {
            incrementerHorloge();
            Message msg = new Message(type, horlogeLamport, id);
            for (Processus p : autres) {
                if (p.id != id && !p.enPanne) { // Si le destinataire n'est pas en panne
                    canal.envoyerMessage(this, p, msg);
                }
            }
            return msg;
        }

        // Réception d’un message
        public synchronized void recevoirMessage(Message msg) {
            if (enPanne) return; // Ignore si le processus est en panne
            majHorloge(msg.horloge);
            switch (msg.type) {
                case REQUEST:
                    fileRequetes.add(msg);
                    // Répond immédiatement avec un message REPLY
                    canal.envoyerMessage(this, trouverProcessus(msg.idSource),
                            new Message(TypeMessage.REPLY, horlogeLamport, id));
                    break;
                case REPLY:
                    reponsesRecues.add(msg.idSource);
                    break;
                case RELEASE:
                    fileRequetes.removeIf(m -> m.idSource == msg.idSource);
                    break;
            }
        }

        // Trouve un processus par son ID
        private Processus trouverProcessus(int id) {
            return autres.stream().filter(p -> p.id == id).findFirst().orElse(null);
        }

        // Tentative d’entrée dans la section critique
        public void demanderSectionCritique() throws InterruptedException {
            if (enPanne) return;
            System.out.println("P" + id + " demande la section critique");
            reponsesRecues.clear();
            fileRequetes.add(diffuserMessage(TypeMessage.REQUEST));
            // Attente des réponses de tous les autres
            while (!enPanne && !toutesReponsesRecues()) {
                Thread.sleep(200);
            }
            // Attente d’être premier dans la file
            while (!enPanne) {
                fileRequetes.removeIf(m -> trouverProcessus(m.idSource).enPanne);
                Message premier = fileRequetes.peek();
                if (premier != null && premier.idSource == id) break;
                Thread.sleep(100);
            }
            if (enPanne) return;
            entrerSectionCritique();
            if (enPanne) return;
            sortirSectionCritique();
        }

        private boolean toutesReponsesRecues() {
            return autres.stream().allMatch(p -> p.id == id || p.enPanne || reponsesRecues.contains(p.id));
        }

        // Entrée en section critique
        private void entrerSectionCritique() throws InterruptedException {
            System.out.println("P" + id + " entre en section critique (horloge=" + horlogeLamport + ")");
            Thread.sleep(1000 + new Random().nextInt(1000));
        }

        // Sortie de section critique
        private void sortirSectionCritique() {
            System.out.println("P" + id + " quitte la section critique");
            fileRequetes.removeIf(m -> m.idSource == id);
            diffuserMessage(TypeMessage.RELEASE);
            // Affichage de la file courante
            afficherFile();
        }

        private void afficherFile() {
            System.out.print("File de P" + id + " : [");
            fileRequetes.stream().sorted(ORDRE_LAMPORT).forEach(m -> System.out.print("P" + m.idSource + " "));
            System.out.println("]");
        }

        @Override
        public void run() {
            try {
                while (!enPanne) {
                    demanderSectionCritique();
                    Thread.sleep(2000);
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    // ---------- MAIN ----------
    public static void main(String[] args) throws InterruptedException {
        CanalReseau canal = new CanalReseau();

        // Création des processus
        Processus p1 = new Processus(1, canal);
        Processus p2 = new Processus(2, canal);
        Processus p3 = new Processus(3, canal);

        // Configuration du réseau (chaque processus connaît les autres)
        List<Processus> tous = Arrays.asList(p1, p2, p3);
        for (Processus p : tous) {
            p.autres = new ArrayList<>(tous);
        }

        // Démarrage des threads
        new Thread(p1).start();
        new Thread(p2).start();
        new Thread(p3).start();

        // Après quelques secondes, simuler une panne
        Thread.sleep(8000);
        System.out.println("P2 tombe en panne !");
        p2.setEnPanne(true);

        // Laisser tourner la simulation
        Thread.sleep(10000);

        System.out.println("\n===== STATISTIQUES =====");
        System.out.println("Nombre total de messages échangés : " + canal.getNombreMessages());
        System.out.println("=========================");
        System.exit(0);
    }
}
