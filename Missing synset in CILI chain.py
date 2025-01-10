import xml.etree.ElementTree as ET
from collections import defaultdict
import os
import multiprocessing as mp
import logging
from collections import deque

# Logimise seadistamine
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(message)s')

def default_synset():
    return {
        "words": set(),
        "cili": None,
        "definition": None,
        "hypernyms": set(),
        "hyponyms": set(),
        "level": None  # Lisatud hierarhia tase
    }

def extract_synsets(file_path):
    synsets = defaultdict(default_synset)

    tree = ET.parse(file_path)
    root = tree.getroot()

    for synset in root.findall(".//Synset"):
        synset_id = synset.get('id')
        ili = synset.get('ili')
        definition = synset.find("Definition")

        if ili:
            synsets[synset_id]["cili"] = ili
        if definition is not None:
            synsets[synset_id]["definition"] = definition.text

        for relation in synset.findall("SynsetRelation"):
            rel_type = relation.get('relType')
            target = relation.get('target')
            if rel_type == 'hypernym':
                synsets[synset_id]["hypernyms"].add(target)
                synsets[target]["hyponyms"].add(synset_id)

    for lexical_entry in root.findall(".//LexicalEntry"):
        lemma = lexical_entry.find("Lemma")
        senses = lexical_entry.findall("Sense")

        for sense in senses:
            synset_id = sense.get('synset')
            if synset_id:
                if lemma is not None:
                    written_form = lemma.get('writtenForm')
                    if written_form:
                        synsets[synset_id]["words"].add(written_form)

                if not synsets[synset_id]["words"]:
                    sense_id = sense.get('id')
                    if sense_id:
                        word = sense_id.split('-', 1)[-1].rsplit('-', 1)[0]
                        synsets[synset_id]["words"].add(word)

    return dict(synsets)

def assign_hierarchy_levels(synsets, root_synset_id):
    # Kõikide synsetide tasemed seadistatakse None-iks
    for synset in synsets.values():
        synset['level'] = None
    # Juursynseti tase on 1
    synsets[root_synset_id]['level'] = 1
    # Kasutame BFS-i tasemete määramiseks
    queue = deque([root_synset_id])
    while queue:
        current_synset_id = queue.popleft()
        current_level = synsets[current_synset_id]['level']
        for hyponym_id in synsets[current_synset_id]['hyponyms']:
            if synsets[hyponym_id]['level'] is None or synsets[hyponym_id]['level'] > current_level + 1:
                synsets[hyponym_id]['level'] = current_level + 1
                queue.append(hyponym_id)

def print_differences(differences, focus_synsets, reference_synsets, focus_cili_to_synset, output_file=None, extra_concepts_file=None):
    def write_output(text):
        print(text)
        if output_file:
            output_file.write(text + '\n')

    # Initsialiseeri loendur "Erinevus leitud" plokkidele
    difference_counter = 0

    # Komplekt juba nähtud "Referents tee" kahe noole vaheliste plokkide jaoks
    seen_reference_paths = set()

    for diff in differences:
        # Kontrollime, kas extra_concepts sisaldab vähemalt ühte CILI, mis on fookuswordnetis
        extra_concepts_in_focus = [cili for cili in diff['extra_concepts'] if cili in focus_cili_to_synset]
        if not extra_concepts_in_focus:
            logging.debug("No extra concepts are in the focus wordnet. Skipping this difference block.")
            continue

        # Ekstraheerime "Referents tee" kahe noole vahelise ploki
        start_idx = min(diff['ref_diff_start'], diff['ref_diff_end'])
        end_idx = max(diff['ref_diff_start'], diff['ref_diff_end'])
        reference_path_between_arrows = diff['reference_path'][start_idx:end_idx+1]

        # Loome võtme synset_id ja CILI põhjal
        reference_path_key = tuple((synset_id, cili) for synset_id, cili, _ in reference_path_between_arrows)

        # Kontrollime, kas see plokk on juba nähtud
        if reference_path_key in seen_reference_paths:
            logging.debug("This reference path between arrows has already been seen. Skipping this difference block.")
            continue
        else:
            seen_reference_paths.add(reference_path_key)

        # Prindi erinevused
        difference_counter += 1
        write_output(f"{difference_counter}. Erinevus leitud:")
        #focus_level = focus_synsets[diff['focus_synset']]['level']
        write_output(f"  Fookus synset: {diff['focus_synset']} ({', '.join(focus_synsets[diff['focus_synset']]['words'])})")
        write_output(f"  Referents synset: {diff['reference_synset']} ({', '.join(reference_synsets[diff['reference_synset']]['words'])})")
        write_output("  Fookus tee:")
        for i, (synset_id, cili) in enumerate(diff['focus_path']):
            words = ', '.join(focus_synsets[synset_id]['words'])
            level = focus_synsets[synset_id]['level']
            prefix = "    "
            if i == diff['focus_diff_start']:
                prefix = " -> "
            elif i == diff['focus_diff_end']:
                prefix = " -> "
            write_output(f"{prefix}{synset_id} ({words}) - Tase: {level} - CILI: {cili}")
        write_output("  Referents tee:")
        for i, (synset_id, cili, (focus_id, focus_words)) in enumerate(diff['reference_path']):
            words = ', '.join(reference_synsets[synset_id]['words'])
            prefix = "    "
            if i == diff['ref_diff_start']:
                prefix = " -> "
            elif i == diff['ref_diff_end']:
                prefix = " -> "
            if focus_id:
                #focus_level = focus_synsets[focus_id]['level']
                focus_info = f" [Fookus: {focus_id} ({', '.join(focus_words)})]"
            else:
                focus_info = ""
            write_output(f"{prefix}{synset_id} ({words}) - CILI: {cili}{focus_info}")
        write_output(f"  Lisatud mõisted referentsis: {', '.join(diff['extra_concepts'])}")
        write_output("")

        # Kirjutame eraldi faili iga extra_concepts_in_focus kohta
        if extra_concepts_file:
            for cili in extra_concepts_in_focus:
                focus_id, focus_words = focus_cili_to_synset[cili]
                #focus_level = focus_synsets[focus_id]['level']
                extra_concept_info = f"CILI: {cili} [Fookus: {focus_id} ({', '.join(focus_words)})]"
                extra_concepts_file.write(extra_concept_info + '\n')

def compare_wordnets_parallel(focus_wordnet, reference_wordnet):
    focus_synsets = extract_synsets(focus_wordnet)
    reference_synsets = extract_synsets(reference_wordnet)

    # Määrame hierarhia tasemed fookuswordnetis
    #root_synset_id = 'estwn-et-8471-n'  # Juursynseti ID
    #assign_hierarchy_levels(focus_synsets, root_synset_id)

    # Loome CILI->fookus synseti sõnastiku
    focus_cili_to_synset = {}
    for synset_id, synset_data in focus_synsets.items():
        cili = synset_data['cili']
        if cili:
            focus_cili_to_synset[cili] = (synset_id, synset_data['words'])

    pool = mp.Pool(processes=mp.cpu_count())
    args = [(synset_id, synset_data, focus_synsets, reference_synsets, focus_cili_to_synset)
            for synset_id, synset_data in focus_synsets.items()]

    results = pool.map(compare_synset, args)
    pool.close()
    pool.join()

    differences = merge_differences(results)
    return differences, focus_synsets, reference_synsets, focus_cili_to_synset

def compare_synset(args):
    synset_id, synset_data, focus_synsets, reference_synsets, focus_cili_to_synset = args
    differences = defaultdict(list)

    if synset_data['cili']:
        focus_paths = find_cili_path(synset_id, focus_synsets)
        for focus_path in focus_paths:
            focus_cili_path = [cili for _, cili in focus_path if cili]

            for ref_synset_id, ref_synset_data in reference_synsets.items():
                if ref_synset_data['cili'] == synset_data['cili']:
                    ref_paths = find_cili_path(ref_synset_id, reference_synsets)
                    for ref_path in ref_paths:
                        ref_cili_path = [cili for _, cili in ref_path if cili]

                        for i in range(len(focus_cili_path) - 1):
                            focus_start = focus_cili_path[i]
                            focus_end = focus_cili_path[i + 1]

                            if focus_start in ref_cili_path and focus_end in ref_cili_path:
                                ref_start_index = ref_cili_path.index(focus_start)
                                ref_end_index = ref_cili_path.index(focus_end)

                                if ref_end_index - ref_start_index > 1:
                                    extra_concepts = ref_cili_path[ref_start_index + 1:ref_end_index]

                                    enhanced_ref_path = [(synset_id, cili, focus_cili_to_synset.get(cili, (None, set())))
                                                         for synset_id, cili in ref_path]

                                    diff = {
                                        'focus_synset': synset_id,
                                        'reference_synset': ref_synset_id,
                                        'focus_path': focus_path,
                                        'reference_path': enhanced_ref_path,
                                        'focus_diff_start': i,
                                        'focus_diff_end': i + 1,
                                        'ref_diff_start': ref_start_index,
                                        'ref_diff_end': ref_end_index,
                                        'extra_concepts': extra_concepts
                                    }

                                    key = (synset_id, ref_synset_id)
                                    differences[key].append(diff)

    return differences

def find_cili_path(synset_id, synsets, path=None):
    if path is None:
        path = []

    current_synset = synsets[synset_id]
    current_path = path + [(synset_id, current_synset['cili'])]

    if not current_synset['hypernyms']:
        return [current_path]

    all_paths = []
    for hypernym in current_synset['hypernyms']:
        all_paths.extend(find_cili_path(hypernym, synsets, current_path))

    return all_paths

def merge_differences(all_differences):
    merged = defaultdict(list)
    for diff_dict in all_differences:
        for key, diffs in diff_dict.items():
            merged[key].extend(diffs)

    unique_differences = []
    for diffs in merged.values():
        # Valime lühima teega erinevuse
        shortest_diff = min(diffs, key=lambda x: len(x['focus_path']) + len(x['reference_path']))
        unique_differences.append(shortest_diff)

    return unique_differences

if __name__ == '__main__':
    # Kasutamine
    focus_wordnet = r"C:\Users\kasutaja\Anaconda__Projects\...\wordnets\estwn-et-2.6.0.xml"
    reference_wordnet = r"C:\Users\kasutaja\Anaconda__Projects\...\wordnets\english-wordnet-2023.xml"
    output_file_path = r"C:\Users\kasutaja\Anaconda__Projects\...\wordnet_comparison_results2.txt"
    extra_concepts_file_path = r"C:\Users\kasutaja\Anaconda__Projects\...\extra_concepts_in_focus2.txt"

    try:
        print("Starting WordNet comparison...")
        focus_synsets = extract_synsets(focus_wordnet)
        #add_manual_hypernym_relations(focus_synsets)
        differences, focus_synsets, reference_synsets, focus_cili_to_synset = compare_wordnets_parallel(focus_wordnet, reference_wordnet)
        print(f"Comparison completed. Number of differences found: {len(differences)}")
        print("Writing results to file...")
        with open(output_file_path, 'w', encoding='utf-8') as output_file, \
             open(extra_concepts_file_path, 'w', encoding='utf-8') as extra_concepts_file:
            output_file.write("Comparison results\n")
            output_file.write(f"Focus WordNet: {os.path.basename(focus_wordnet)}\n")
            output_file.write(f"Reference WordNet: {os.path.basename(reference_wordnet)}\n")
            output_file.write(f"Number of differences found: {len(differences)}\n\n")
            print_differences(differences, focus_synsets, reference_synsets, focus_cili_to_synset, output_file, extra_concepts_file)
        print(f"\nResults have been saved to file: {output_file_path}")
        print(f"Additional concepts in focus wordnet have been saved to file: {extra_concepts_file_path}")
    except FileNotFoundError as e:
        print(f"Error: File not found. {str(e)}")
    except ET.ParseError:
        print("Error: XML parsing error occurred. Please ensure the files are in correct format.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
