import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

class Message {
    public enum Type { REQUEST, REPLY, RELEASE }
    Type type;
    int senderId;
    int timestamp;

    public Message(Type type, int senderId, int timestamp) {
        this.type = type;
        this.senderId = senderId;
        this.timestamp = timestamp;
    }
}

class Processus extends Thread {
    private int id;
    private int clock = 0;
    private List<Processus> autres;
    private Queue<Message> fileMessages = new ConcurrentLinkedQueue<>();
    private Set<Integer> reponsesRecues = new HashSet<>();
    private List<Message> fileDemandes = new ArrayList<>();

    public Processus(int id) { this.id = id; }

    public void setAutres(List<Processus> autres) { this.autres = autres; }

    private synchronized void tick() { clock++; }

    private synchronized void majHorloge(int autreHorloge) {
        clock = Math.max(clock, autreHorloge) + 1;
    }

    public synchronized void recoitMessage(Message m) {
        majHorloge(m.timestamp);
        fileMessages.add(m);
    }

    private void broadcast(Message.Type type) {
        tick();
        Message m = new Message(type, id, clock);
        for (Processus p : autres)
            if (p.id != this.id)
                p.recoitMessage(m);
    }

    private boolean estPremierDansFile() {
        if (fileDemandes.isEmpty()) return false;
        Message moi = null;
        for (Message m : fileDemandes)
            if (m.senderId == id) { moi = m; break; }
        if (moi == null) return false;
        for (Message m : fileDemandes) {
            if (m.senderId != id) {
                if (m.timestamp < moi.timestamp) return false;
                if (m.timestamp == moi.timestamp && m.senderId < id) return false;
            }
        }
        return true;
    }

    private void demandeSC() throws InterruptedException {
        tick();
        Message demande = new Message(Message.Type.REQUEST, id, clock);
        synchronized (this) { fileDemandes.add(demande); }
        System.out.println("Processus " + id + " demande SC à t=" + clock);
        broadcast(Message.Type.REQUEST);
        reponsesRecues.clear();
        reponsesRecues.add(id);

        while (reponsesRecues.size() < autres.size() || !estPremierDansFile()) {
            traiterMessages();
            Thread.sleep(50);
        }
        entrerSC();
    }

    private void entrerSC() throws InterruptedException {
        System.out.println("==> Processus " + id + " entre en SC (t=" + clock + ")");
        Thread.sleep(1000);
        sortirSC();
    }

    private void sortirSC() {
        System.out.println("<== Processus " + id + " quitte SC (t=" + clock + ")");
        synchronized (this) { fileDemandes.removeIf(m -> m.senderId == id); }
        broadcast(Message.Type.RELEASE);
    }

    private void traiterMessages() {
        while (!fileMessages.isEmpty()) {
            Message m = fileMessages.poll();
            switch (m.type) {
                case REQUEST:
                    synchronized (this) { fileDemandes.add(m); }
                    tick();
                    autres.stream().filter(p -> p.id == m.senderId).findFirst()
                            .ifPresent(p -> p.recoitMessage(new Message(Message.Type.REPLY, id, clock)));
                    break;
                case REPLY:
                    reponsesRecues.add(m.senderId);
                    break;
                case RELEASE:
                    synchronized (this) { fileDemandes.removeIf(x -> x.senderId == m.senderId); }
                    break;
            }
        }
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < 3; i++) {
                Thread.sleep(500 + new Random().nextInt(1000));
                demandeSC();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

public class LamportExclusion {
    public static void main(String[] args) {
        int N = 3;
        List<Processus> liste = new ArrayList<>();
        for (int i = 0; i < N; i++) liste.add(new Processus(i));
        for (Processus p : liste) p.setAutres(liste);
        for (Processus p : liste) p.start();
    }
}