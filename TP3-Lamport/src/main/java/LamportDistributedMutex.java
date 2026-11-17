import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
public class LamportDistributedMutex {

    // Nombre total de processus simulés
    private static final int NB_PROCESSUS = 3;
    private static final double PROBABILITE_PERTE = 0.05;
    private static final double PROBABILITE_PANNE = 0.1;
    private static final AtomicLong tempsAttenteTotal = new AtomicLong(0);
    private static final AtomicInteger nbAccesSC = new AtomicInteger(0);
    private static final Comparator<Message> ORDRE_LAMPORT =
            Comparator.comparingInt((Message m) -> m.horloge).thenComparingInt(m -> m.idSource);

    // Canal réseau simulé (messages envoyés avec délai)
    static class CanalReseau {
        private final Random random = new Random();
        private final AtomicInteger compteurMessages = new AtomicInteger(0);
        private final AtomicInteger compteurPertes = new AtomicInteger(0);
        private final Map<String, ExecutorService> liens = new ConcurrentHashMap<>();

        // Envoi d’un message avec délai simulé
        public void envoyerMessage(Processus src, Processus dest, Message msg) {
            compteurMessages.incrementAndGet(); // Incrémenter le compteur global
            int delai = 300 + random.nextInt(500); // Délai aléatoire entre 300 et 800 ms
            if (random.nextDouble() < PROBABILITE_PERTE) {
                compteurPertes.incrementAndGet();
                System.out.println("Message " + msg.type + " de P" + src.id + " vers P" + dest.id + " perdu !");
                return;
            }
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
        public int getNombrePertes() {
            return compteurPertes.get();
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
            long debut = System.currentTimeMillis();
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
            long attente = System.currentTimeMillis() - debut;
            tempsAttenteTotal.addAndGet(attente);
            nbAccesSC.incrementAndGet();
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
            Random random = new Random();
            try {
                while (!enPanne) {
                    demanderSectionCritique();
                    Thread.sleep(2000);
                    if (random.nextDouble() < PROBABILITE_PANNE) {
                        System.out.println("P" + id + " tombe en panne !");
                        setEnPanne(true);
                    }
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    // ---------- MAIN ----------
    public static void main(String[] args) throws InterruptedException {
        CanalReseau canal = new CanalReseau();

        // Création des processus
        List<Processus> tous = new ArrayList<>();
        for (int i = 1; i <= NB_PROCESSUS; i++) {
            tous.add(new Processus(i, canal));
        }

        // Configuration du réseau (chaque processus connaît les autres)
        for (Processus p : tous) {
            p.autres = new ArrayList<>(tous);
        }

        // Démarrage des threads
        for (Processus p : tous) {
            new Thread(p).start();
        }

        // Laisser tourner la simulation
        Thread.sleep(18000);

        System.out.println("\n===== STATISTIQUES =====");
        System.out.println("Nombre de processus : " + NB_PROCESSUS);
        System.out.println("Nombre total de messages échangés : " + canal.getNombreMessages());
        System.out.println("Nombre de messages perdus : " + canal.getNombrePertes());
        System.out.println("Nombre d'accès à la SC : " + nbAccesSC.get());
        System.out.println("Temps moyen d'attente pour entrer en SC : "
                + (nbAccesSC.get() == 0 ? 0 : tempsAttenteTotal.get() / nbAccesSC.get()) + " ms");
        System.out.println("=========================");
        System.exit(0);
    }
}
